// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// tdrTime returns the GTFS time for a number of seconds since midnight.
func tdrTime(sec int) gtfs.Time {
	return gtfs.Time{Hour: int16(sec / 3600), Minute: int8(sec % 3600 / 60), Second: int8(sec % 60)}
}

// tdrServiceOn returns a service (without calendar.txt entry) that is active on the
// given days of January 2026.
func tdrServiceOn(id string, days ...int) *gtfs.Service {
	s := gtfs.EmptyService()
	s.SetId(id)

	for _, d := range days {
		s.SetExceptionTypeOn(gtfs.NewDate(uint8(d), 1, 2026), 1)
	}

	return s
}

// tdrActiveDays returns the days of January 2026 on which the service is active.
func tdrActiveDays(s *gtfs.Service) []int {
	days := make([]int, 0)

	for d := 1; d <= 31; d++ {
		if s.IsActiveOn(gtfs.NewDate(uint8(d), 1, 2026)) {
			days = append(days, d)
		}
	}

	return days
}

// tdrAddTrip adds a trip on the route and service to the feed. The first stop time
// departs at the given number of seconds since midnight, and each of the following
// ones is one minute after the previous one.
func tdrAddTrip(feed *gtfsparser.Feed, id string, route *gtfs.Route, service *gtfs.Service, start int, stops ...*gtfs.Stop) *gtfs.Trip {
	headsign := "Hbf"
	trip := &gtfs.Trip{Id: id, Route: route, Service: service, Headsign: &headsign}

	for i, s := range stops {
		var st gtfs.StopTime
		st.SetStop(s)
		st.SetSequence(i + 1)
		st.SetArrival_time(tdrTime(start + i*60))
		st.SetDeparture_time(tdrTime(start + i*60))
		trip.StopTimes = append(trip.StopTimes, st)
	}

	feed.Trips[id] = trip

	if _, ok := feed.Services[service.Id()]; !ok {
		feed.Services[service.Id()] = service
	}

	return trip
}

func tdrRun(feed *gtfsparser.Feed) {
	TripDuplicateRemover{MaxDayDist: 7}.Run(feed)
}

func TestTripDuplicateRemoverEqualTrips(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a"}
	b := &gtfs.Stop{Id: "b"}
	route := &gtfs.Route{Id: "r", Type: 3}
	service := tdrServiceOn("s", 5, 6, 7)

	tdrAddTrip(feed, "t1", route, service, 8*3600, a, b)
	tdrAddTrip(feed, "t2", route, service, 8*3600, a, b)
	tdrAddTrip(feed, "t3", route, service, 8*3600, a, b)
	tdrAddTrip(feed, "other", route, service, 9*3600, a, b)

	tdrRun(feed)

	if len(feed.Trips) != 2 {
		t.Fatalf("expected the three equal trips to be merged into one, got %d trips", len(feed.Trips))
	}

	if _, ok := feed.Trips["other"]; !ok {
		t.Error("the trip with a different departure time should not be merged")
	}
}

func TestTripDuplicateRemoverContainedTrips(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a"}
	b := &gtfs.Stop{Id: "b"}
	route := &gtfs.Route{Id: "r", Type: 3}

	tdrAddTrip(feed, "all", route, tdrServiceOn("s1", 5, 6, 7, 8, 9), 8*3600, a, b)
	tdrAddTrip(feed, "some", route, tdrServiceOn("s2", 6, 7), 8*3600, a, b)

	tdrRun(feed)

	if len(feed.Trips) != 1 {
		t.Fatalf("expected the contained trip to be removed, got %d trips", len(feed.Trips))
	}

	trip, ok := feed.Trips["all"]
	if !ok {
		t.Fatal("expected the trip that serves more days to be kept")
	}

	if days := tdrActiveDays(trip.Service); fmt.Sprint(days) != "[5 6 7 8 9]" {
		t.Errorf("expected the trip to run on [5 6 7 8 9], got %v", days)
	}
}

func TestTripDuplicateRemoverOverlappingTrips(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a"}
	b := &gtfs.Stop{Id: "b"}
	route := &gtfs.Route{Id: "r", Type: 3}

	tdrAddTrip(feed, "t1", route, tdrServiceOn("s1", 5, 6, 7), 8*3600, a, b)
	tdrAddTrip(feed, "t2", route, tdrServiceOn("s2", 7, 8, 9), 8*3600, a, b)

	tdrRun(feed)

	// each day has to be served by exactly one trip
	served := make(map[int]int)

	for _, trip := range feed.Trips {
		for _, d := range tdrActiveDays(trip.Service) {
			served[d]++
		}
	}

	for d := 5; d <= 9; d++ {
		if served[d] != 1 {
			t.Errorf("expected day %d to be served by exactly one trip, got %d", d, served[d])
		}
	}

	if len(served) != 5 {
		t.Errorf("expected the trips to run on 5 days, got %v", served)
	}
}

func TestTripDuplicateRemoverAdjacentTrips(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a"}
	b := &gtfs.Stop{Id: "b"}
	route := &gtfs.Route{Id: "r", Type: 3}

	// Monday to Friday of two consecutive weeks
	week1 := gtfs.EmptyService()
	week1.SetId("week1")
	week1.SetRawDaymap(0x3e)
	week1.SetStart_date(gtfs.NewDate(5, 1, 2026))
	week1.SetEnd_date(gtfs.NewDate(9, 1, 2026))

	week2 := gtfs.EmptyService()
	week2.SetId("week2")
	week2.SetRawDaymap(0x3e)
	week2.SetStart_date(gtfs.NewDate(12, 1, 2026))
	week2.SetEnd_date(gtfs.NewDate(16, 1, 2026))

	tdrAddTrip(feed, "t1", route, week1, 8*3600, a, b)
	tdrAddTrip(feed, "t2", route, week2, 8*3600, a, b)

	tdrRun(feed)

	if len(feed.Trips) != 1 {
		t.Fatalf("expected the adjacent trips to be merged, got %d trips", len(feed.Trips))
	}

	for _, trip := range feed.Trips {
		if days := tdrActiveDays(trip.Service); fmt.Sprint(days) != "[5 6 7 8 9 12 13 14 15 16]" {
			t.Errorf("expected the trip to run on both weeks, got %v", days)
		}
	}
}

// Trips that don't have an equal trip are not touched.
func TestTripDuplicateRemoverUniqueTrips(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a"}
	b := &gtfs.Stop{Id: "b"}
	route := &gtfs.Route{Id: "r", Type: 3}
	service := tdrServiceOn("s", 5, 6, 7)

	for i := 0; i < 500; i++ {
		tdrAddTrip(feed, fmt.Sprintf("trip%d", i), route, service, 8*3600+i*60, a, b)
	}

	// and one trip that has an equal trip
	tdrAddTrip(feed, "dup1", route, service, 20*3600, a, b)
	tdrAddTrip(feed, "dup2", route, service, 20*3600, a, b)

	tdrRun(feed)

	if len(feed.Trips) != 501 {
		t.Errorf("expected 501 trips, got %d", len(feed.Trips))
	}
}

// tdrServedDays returns, for each kind of trip (same route, headsign, stops and
// times), the days on which it is served by any of the trips in the feed.
func tdrServedDays(feed *gtfsparser.Feed) map[string]map[int]bool {
	served := make(map[string]map[int]bool)

	for _, trip := range feed.Trips {
		sig := fmt.Sprintf("%s|%s|", trip.Route.Id, *trip.Headsign)

		for i := range trip.StopTimes {
			st := &trip.StopTimes[i]
			sig += fmt.Sprintf("%s@%d,", st.Stop().Id, st.Arrival_time().SecondsSinceMidnight())
		}

		if served[sig] == nil {
			served[sig] = make(map[int]bool)
		}

		for _, d := range tdrActiveDays(trip.Service) {
			served[sig][d] = true
		}
	}

	return served
}

// Whichever of the trips are merged, a trip has to be served on the same days as
// before.
func TestTripDuplicateRemoverKeepsServedDays(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		rnd := rand.New(rand.NewSource(seed))
		feed := gtfsparser.NewFeed()

		stops := []*gtfs.Stop{{Id: "a"}, {Id: "b"}, {Id: "c"}}
		routes := []*gtfs.Route{{Id: "r1", Type: 3}, {Id: "r2", Type: 3}}

		for i := 0; i < 60; i++ {
			days := make([]int, 0)
			for d := 1; d <= 21; d++ {
				if rnd.Intn(3) == 0 {
					days = append(days, d)
				}
			}
			if len(days) == 0 {
				days = append(days, 1)
			}

			var service *gtfs.Service
			if rnd.Intn(3) == 0 {
				// a service with a calendar.txt entry for a few weeks, with some exceptions
				service = tdrServiceOn(fmt.Sprintf("s%d", i), days[:1]...)
				service.SetRawDaymap(uint8(rnd.Intn(127) + 1))
				first := rnd.Intn(14) + 1
				service.SetStart_date(gtfs.NewDate(uint8(first), 1, 2026))
				service.SetEnd_date(gtfs.NewDate(uint8(first+rnd.Intn(7)), 1, 2026))
			} else {
				service = tdrServiceOn(fmt.Sprintf("s%d", i), days...)
			}

			route := routes[rnd.Intn(len(routes))]
			start := (8 + rnd.Intn(2)) * 3600
			numStops := 2 + rnd.Intn(2)

			tdrAddTrip(feed, fmt.Sprintf("t%d", i), route, service, start, stops[:numStops]...)
		}

		before := tdrServedDays(feed)

		tdrRun(feed)

		after := tdrServedDays(feed)

		if !reflect.DeepEqual(before, after) {
			t.Fatalf("seed %d: the days on which trips are served changed:\nbefore: %v\nafter:  %v", seed, before, after)
		}
	}
}

// Finding the trips that could be merged must not need memory for each trip.
func TestTripDuplicateRemoverMemory(t *testing.T) {
	// use a single chunk, so that the allocations don't depend on the number of CPUs
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	feed := gtfsparser.NewFeed()

	stops := make([]*gtfs.Stop, 100)
	for i := range stops {
		stops[i] = &gtfs.Stop{Id: fmt.Sprintf("stop%d", i)}
	}

	route := &gtfs.Route{Id: "r", Type: 3}
	service := tdrServiceOn("s", 5, 6, 7)

	// 20000 trips that all differ in their departure time at the first stop
	for i := 0; i < 20000; i++ {
		tdrAddTrip(feed, fmt.Sprintf("trip%d", i), route, service, i, stops[i%len(stops)], stops[(i+1)%len(stops)])
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	tdrRun(feed)

	runtime.ReadMemStats(&after)

	if len(feed.Trips) != 20000 {
		t.Fatalf("expected no trips to be merged, got %d trips", len(feed.Trips))
	}

	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("removing redundant trips allocated %d KiB for 20000 trips", allocated>>10)

	// a map entry and a slice for each trip, in each of the (at least 10) rounds, would be
	// more than 20 MiB
	if allocated > 8<<20 {
		t.Errorf("removing redundant trips allocated %d KiB for 20000 trips, expected less than 8192 KiB", allocated>>10)
	}
}
