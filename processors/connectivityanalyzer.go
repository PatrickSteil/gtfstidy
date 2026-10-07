// Copyright 2024 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"math/bits"
	"os"
	"sort"
)

// maxPrintedComponents and maxPrintedGaps limit the size of the printed report
const maxPrintedComponents = 10
const maxPrintedGaps = 10

// ConnectivityAnalyzer reports on the connectivity of a feed: the connected
// components of the stop network (stops are joined by consecutive stop times
// of a trip, by transfers, and by their parent stations) and gaps in the
// service calendar, both for the feed as a whole and for single stops.
// It does not modify the feed.
type ConnectivityAnalyzer struct {
}

// DayGap is a run of consecutive days without any service
type DayGap struct {
	Start gtfs.Date
	End   gtfs.Date
	Days  int
}

// ConnectivityComponent describes a single connected component
type ConnectivityComponent struct {
	// Number of served stops in this component
	Stops int
	// Number of routes with a trip in this component
	Routes int
	// Number of trips in this component
	Trips int
	// Number of stops that have at least one day without service
	// between their first and last served day
	StopsWithGap int
	// Longest and median (over all stops) such gap, in days
	MaxStopGap    int
	MedianStopGap int
}

// ConnectivityReport is the result of a ConnectivityAnalyzer run
type ConnectivityReport struct {
	// Stops that appear in at least one stop time
	ServedStops int
	// Stops that appear in no stop time
	UnservedStops int
	// Connected components, largest (by stops) first
	Components []ConnectivityComponent
	// First and last day with service, empty if there is no service
	FirstDay gtfs.Date
	LastDay  gtfs.Date
	// Gaps between FirstDay and LastDay in which no trip is served at
	// all, in chronological order
	FeedGaps []DayGap
}

// Run this ConnectivityAnalyzer on some feed
func (f ConnectivityAnalyzer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Analyzing connectivity...")
	rep := f.Analyze(feed)
	fmt.Fprintf(os.Stdout, " done.\n")
	rep.Print(os.Stdout)
}

// Analyze computes the connectivity report for a feed
func (f ConnectivityAnalyzer) Analyze(feed *gtfsparser.Feed) ConnectivityReport {
	idx := make(map[*gtfs.Stop]int, len(feed.Stops))
	for _, s := range feed.Stops {
		idx[s] = len(idx)
	}

	uf := newUnionFind(len(idx))
	served := make([]bool, len(idx))

	for s, i := range idx {
		if s.Parent_station != nil {
			if p, ok := idx[s.Parent_station]; ok {
				uf.union(i, p)
			}
		}
	}

	for k := range feed.Transfers {
		a, okA := idx[k.From_stop]
		b, okB := idx[k.To_stop]
		if okA && okB {
			uf.union(a, b)
		}
	}

	for _, t := range feed.Trips {
		prev := -1
		for i := range t.StopTimes {
			cur, ok := idx[t.StopTimes[i].Stop()]
			if !ok {
				continue
			}
			served[cur] = true
			if prev >= 0 {
				uf.union(prev, cur)
			}
			prev = cur
		}
	}

	var rep ConnectivityReport
	comps := make(map[int]*ConnectivityComponent)
	compOf := func(stop int) *ConnectivityComponent {
		r := uf.find(stop)
		c, ok := comps[r]
		if !ok {
			c = &ConnectivityComponent{}
			comps[r] = c
		}
		return c
	}

	for i := range served {
		if served[i] {
			rep.ServedStops++
			compOf(i).Stops++
		} else {
			rep.UnservedStops++
		}
	}

	routes := make(map[int]map[*gtfs.Route]struct{})
	svcStops := make(map[*gtfs.Service]map[int]struct{})
	for _, t := range feed.Trips {
		first := -1
		for i := range t.StopTimes {
			if s, ok := idx[t.StopTimes[i].Stop()]; ok {
				first = s
				break
			}
		}
		if first < 0 {
			continue
		}

		compOf(first).Trips++
		r := uf.find(first)
		if routes[r] == nil {
			routes[r] = make(map[*gtfs.Route]struct{})
		}
		routes[r][t.Route] = struct{}{}

		if t.Service != nil {
			set := svcStops[t.Service]
			if set == nil {
				set = make(map[int]struct{})
				svcStops[t.Service] = set
			}
			for i := range t.StopTimes {
				if s, ok := idx[t.StopTimes[i].Stop()]; ok {
					set[s] = struct{}{}
				}
			}
		}
	}
	for r, rs := range routes {
		comps[r].Routes = len(rs)
	}

	cal := newServiceCalendar(svcStops)
	rep.FirstDay, rep.LastDay = cal.first, cal.last
	rep.FeedGaps = cal.feedGaps()

	stopGaps := make(map[int][]int)
	for i := range served {
		if served[i] {
			r := uf.find(i)
			stopGaps[r] = append(stopGaps[r], cal.maxStopGap(i))
		}
	}

	for r, c := range comps {
		gaps := stopGaps[r]
		sort.Ints(gaps)
		for _, g := range gaps {
			if g > 0 {
				c.StopsWithGap++
			}
		}
		if len(gaps) > 0 {
			c.MaxStopGap = gaps[len(gaps)-1]
			c.MedianStopGap = gaps[len(gaps)/2]
		}
		rep.Components = append(rep.Components, *c)
	}

	sort.Slice(rep.Components, func(i, j int) bool {
		a, b := rep.Components[i], rep.Components[j]
		if a.Stops != b.Stops {
			return a.Stops > b.Stops
		}
		return a.Trips > b.Trips
	})

	return rep
}

// Print writes a human-readable version of the report
func (r ConnectivityReport) Print(out *os.File) {
	total := r.ServedStops + r.UnservedStops
	fmt.Fprintf(out, "\nConnectivity report\n")
	fmt.Fprintf(out, "  stops:                %d (%d served, %d not served by any trip)\n", total, r.ServedStops, r.UnservedStops)
	fmt.Fprintf(out, "  connected components: %d\n", len(r.Components))

	if len(r.Components) > 0 && r.ServedStops > 0 {
		l := r.Components[0]
		fmt.Fprintf(out, "  largest component:    %d stops (%.1f%% of served stops), %d routes, %d trips\n",
			l.Stops, 100.0*float64(l.Stops)/float64(r.ServedStops), l.Routes, l.Trips)
		single := 0
		for _, c := range r.Components {
			if c.Stops == 1 {
				single++
			}
		}
		fmt.Fprintf(out, "  single-stop components: %d\n", single)

		fmt.Fprintf(out, "\n  %-4s %8s %8s %8s %12s %10s %10s\n", "#", "stops", "routes", "trips", "stops w/ gap", "max gap", "median gap")
		for i, c := range r.Components {
			if i >= maxPrintedComponents {
				fmt.Fprintf(out, "  ... %d more components\n", len(r.Components)-i)
				break
			}
			fmt.Fprintf(out, "  %-4d %8d %8d %8d %12d %8dd %8dd\n", i+1, c.Stops, c.Routes, c.Trips, c.StopsWithGap, c.MaxStopGap, c.MedianStopGap)
		}
	}

	fmt.Fprintf(out, "\nService calendar\n")
	if r.FirstDay.IsEmpty() {
		fmt.Fprintf(out, "  no service found\n")
		return
	}
	fmt.Fprintf(out, "  first / last day with service: %s / %s\n", fmtDate(r.FirstDay), fmtDate(r.LastDay))
	if len(r.FeedGaps) == 0 {
		fmt.Fprintf(out, "  no days without service in between\n")
		return
	}

	gapDays := 0
	for _, g := range r.FeedGaps {
		gapDays += g.Days
	}
	fmt.Fprintf(out, "  days without any service in between: %d, in %d gaps\n", gapDays, len(r.FeedGaps))

	longest := make([]DayGap, len(r.FeedGaps))
	copy(longest, r.FeedGaps)
	sort.SliceStable(longest, func(i, j int) bool { return longest[i].Days > longest[j].Days })
	if len(longest) > maxPrintedGaps {
		longest = longest[:maxPrintedGaps]
	}
	fmt.Fprintf(out, "  longest gaps:\n")
	for _, g := range longest {
		fmt.Fprintf(out, "    %s to %s (%d days)\n", fmtDate(g.Start), fmtDate(g.End), g.Days)
	}
}

func fmtDate(d gtfs.Date) string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
}

// serviceCalendar holds, for every stop, a bitset of the days on which the
// stop is served. Day 0 is the first day with any defined service.
type serviceCalendar struct {
	first, last gtfs.Date
	days        int
	dates       []gtfs.Date
	all         []uint64
	stops       map[int][]uint64
}

// newServiceCalendar builds the calendar from the stops served by each service
func newServiceCalendar(svcStops map[*gtfs.Service]map[int]struct{}) *serviceCalendar {
	c := &serviceCalendar{stops: make(map[int][]uint64)}

	var lo, hi gtfs.Date
	for s := range svcStops {
		a, b := s.GetFirstDefinedDate(), s.GetLastDefinedDate()
		if a.IsEmpty() || b.IsEmpty() {
			continue
		}
		if lo.IsEmpty() || a.GetTime().Before(lo.GetTime()) {
			lo = a
		}
		if hi.IsEmpty() || b.GetTime().After(hi.GetTime()) {
			hi = b
		}
	}
	if lo.IsEmpty() {
		return c
	}

	c.days = int(hi.GetTime().Sub(lo.GetTime()).Hours()/24+0.5) + 1
	words := (c.days + 63) / 64
	c.dates = make([]gtfs.Date, c.days)
	for i := range c.dates {
		c.dates[i] = lo.GetOffsettedDate(i)
	}

	c.all = make([]uint64, words)
	for s, stops := range svcStops {
		sb := make([]uint64, words)
		for i, d := range c.dates {
			if s.IsActiveOn(d) {
				sb[i/64] |= 1 << uint(i%64)
			}
		}
		for w := range sb {
			c.all[w] |= sb[w]
		}
		for stop := range stops {
			b := c.stops[stop]
			if b == nil {
				b = make([]uint64, words)
				c.stops[stop] = b
			}
			for w := range sb {
				b[w] |= sb[w]
			}
		}
	}

	if f, l, ok := bitRange(c.all); ok {
		c.first, c.last = c.dates[f], c.dates[l]
	}
	return c
}

// bitRange returns the index of the first and last set bit
func bitRange(b []uint64) (int, int, bool) {
	first, last := -1, -1
	for w := range b {
		if b[w] == 0 {
			continue
		}
		if first < 0 {
			first = w*64 + bits.TrailingZeros64(b[w])
		}
		last = w*64 + 63 - bits.LeadingZeros64(b[w])
	}
	return first, last, first >= 0
}

// forEachGap calls fn for every run of unset bits between the first and last
// set bit of b, with the index of the run's first bit and its length
func forEachGap(b []uint64, fn func(start, length int)) {
	first, last, ok := bitRange(b)
	if !ok {
		return
	}
	run := 0
	for i := first; i <= last; i++ {
		if b[i/64]&(1<<uint(i%64)) != 0 {
			if run > 0 {
				fn(i-run, run)
				run = 0
			}
		} else {
			run++
		}
	}
}

// maxStopGap returns the longest run of days without service at a stop,
// between the first and last day it is served
func (c *serviceCalendar) maxStopGap(stop int) int {
	m := 0
	forEachGap(c.stops[stop], func(_, l int) {
		if l > m {
			m = l
		}
	})
	return m
}

// feedGaps returns all runs of days without any service in the feed, between
// the first and last day with service
func (c *serviceCalendar) feedGaps() []DayGap {
	var ret []DayGap
	forEachGap(c.all, func(start, l int) {
		ret = append(ret, DayGap{c.dates[start], c.dates[start+l-1], l})
	})
	return ret
}

// unionFind is a disjoint-set forest with path halving and union by size
type unionFind struct {
	parent []int
	size   []int
}

func newUnionFind(n int) *unionFind {
	u := &unionFind{make([]int, n), make([]int, n)}
	for i := range u.parent {
		u.parent[i] = i
		u.size[i] = 1
	}
	return u
}

func (u *unionFind) find(x int) int {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]]
		x = u.parent[x]
	}
	return x
}

func (u *unionFind) union(a, b int) {
	a, b = u.find(a), u.find(b)
	if a == b {
		return
	}
	if u.size[a] < u.size[b] {
		a, b = b, a
	}
	u.parent[b] = a
	u.size[a] += u.size[b]
}
