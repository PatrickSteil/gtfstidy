// Copyright 2025 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"container/heap"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

func NumTrips(f *gtfs.Frequency) int {
	duration := int(f.End_time.SecondsSinceMidnight()) - int(f.Start_time.SecondsSinceMidnight())
	if duration < 0 || f.Headway_secs <= 0 {
		return 0
	}
	return duration / f.Headway_secs
}

func TopLevelStop(stop *gtfs.Stop, feed *gtfsparser.Feed) *gtfs.Stop {
	for stop.Parent_station != nil {
		stop = stop.Parent_station
	}
	return stop
}

type ScoredStop struct {
	Stop       *gtfs.Stop
	Importance int
	Index      int // original order
}

type StopHeap []ScoredStop

func (h StopHeap) Len() int { return len(h) }
func (h StopHeap) Less(i, j int) bool {
	return h[i].Importance < h[j].Importance
}
func (h StopHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *StopHeap) Push(x interface{}) { *h = append(*h, x.(ScoredStop)) }
func (h *StopHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type ExtendRouteName struct {
}

func (f ExtendRouteName) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Extending route names ... ")

	const K = 4
	workerCount := runtime.NumCPU()

	var stopImportanceMap sync.Map
	for _, s := range feed.Stops {
		stopImportanceMap.Store(s, new(atomic.Int32))
	}

	tripCh := make(chan *gtfs.Trip)
	var tripWg sync.WaitGroup

	for w := 0; w < workerCount; w++ {
		tripWg.Add(1)
		go func() {
			defer tripWg.Done()
			for t := range tripCh {
				amount := 1
				if t.Frequencies != nil {
					for _, freq := range *t.Frequencies {
						amount += NumTrips(freq)
					}
				}
				for _, st := range t.StopTimes {
					parentStop := TopLevelStop(st.Stop(), feed)
					if v, ok := stopImportanceMap.Load(parentStop); ok {
						v.(*atomic.Int32).Add(int32(amount))
					}
				}
			}
		}()
	}

	for _, t := range feed.Trips {
		tripCh <- t
	}
	close(tripCh)
	tripWg.Wait()

	oneTripOfRoute := make(map[string]*gtfs.Trip)

	for _, t := range feed.Trips {
		// set the trip as representative of its route
		_, ok := oneTripOfRoute[t.Route.Id]

		if !ok {
			oneTripOfRoute[t.Route.Id] = t
		}
	}

	routesCh := make(chan *gtfs.Route)
	var wg sync.WaitGroup

	var newRouteNames atomic.Uint64

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for route := range routesCh {
				if route.Long_name != "" {
					continue
				}

				orderedStops := []*gtfs.Stop{}

				trip := oneTripOfRoute[route.Id]
				for _, st := range trip.StopTimes {
					stop := TopLevelStop(st.Stop(), feed)
					orderedStops = append(orderedStops, stop)
				}

				h := make(StopHeap, 0, K+1)
				heap.Init(&h)

				for idx, stop := range orderedStops {
					importance := 0
					if v, ok := stopImportanceMap.Load(stop); ok {
						importance = int(v.(*atomic.Int32).Load())
					}
					ss := ScoredStop{Stop: stop, Importance: importance, Index: idx}

					heap.Push(&h, ss)
					if h.Len() > K {
						heap.Pop(&h)
					}
				}

				topStops := make([]ScoredStop, h.Len())
				for i := len(topStops) - 1; i >= 0; i-- {
					topStops[i] = heap.Pop(&h).(ScoredStop)
				}
				sort.SliceStable(topStops, func(i, j int) bool {
					return topStops[i].Index < topStops[j].Index
				})

				stopNames := make([]string, 0, K)
				for _, s := range topStops {
					name := s.Stop.Name
					if s.Stop.Parent_station != nil {
						name = s.Stop.Parent_station.Name
					}
					stopNames = append(stopNames, name)
				}
				if len(stopNames) > 0 {
					route.Long_name = stopNames[0]
					for i := 1; i < len(stopNames); i++ {
						route.Long_name += " - " + stopNames[i]
					}

					newRouteNames.Add(1)
				}
			}
		}()
	}

	for _, r := range feed.Routes {
		routesCh <- r
	}
	close(routesCh)

	wg.Wait()

	if newRouteNames.Load() > 0 {
		fmt.Fprintf(os.Stdout, "(+%d route_long_names [+%.2f%%]) done.\n", newRouteNames.Load(), 100*(float64(newRouteNames.Load())/float64(len(feed.Routes))))
	} else {
		fmt.Fprintf(os.Stdout, "done.\n")
	}
}
