// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// sdrAddTrip adds a trip that visits the given stops to the feed.
func sdrAddTrip(feed *gtfsparser.Feed, id string, stops ...*gtfs.Stop) *gtfs.Trip {
	trip := &gtfs.Trip{Id: id}

	for i, s := range stops {
		var st gtfs.StopTime
		st.SetStop(s)
		st.SetSequence(i + 1)
		trip.StopTimes = append(trip.StopTimes, st)
	}

	feed.Trips[id] = trip
	return trip
}

// sdrAssertStopTimesInFeed fails the test for every stop time that references a
// stop that is not part of the feed.
func sdrAssertStopTimesInFeed(t *testing.T, feed *gtfsparser.Feed) {
	t.Helper()

	for _, trip := range feed.Trips {
		for i := range trip.StopTimes {
			s := trip.StopTimes[i].Stop()
			if feed.Stops[s.Id] != s {
				t.Errorf("stop time %d of trip %q references stop %q, which is not in the feed", i, trip.Id, s.Id)
			}
		}
	}
}

func TestStopDuplicateRemoverRewritesStopTimes(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	b := &gtfs.Stop{Id: "b", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	c := &gtfs.Stop{Id: "c", Name: "Elsewhere", Lat: 48.3, Lon: 16.5}

	for _, s := range []*gtfs.Stop{a, b, c} {
		feed.Stops[s.Id] = s
	}

	t1 := sdrAddTrip(feed, "t1", a, c)
	t2 := sdrAddTrip(feed, "t2", b, c)
	t3 := sdrAddTrip(feed, "t3", c, b, a)

	StopDuplicateRemover{DistThresholdStop: 5.0, DistThresholdStation: 50}.Run(feed)

	if len(feed.Stops) != 2 {
		t.Fatalf("expected the duplicate stops to be merged, got %d stops", len(feed.Stops))
	}

	sdrAssertStopTimesInFeed(t, feed)

	// the stop with the smallest id is kept
	expected := map[*gtfs.Trip][]*gtfs.Stop{t1: {a, c}, t2: {a, c}, t3: {c, a, a}}

	for trip, stops := range expected {
		if len(trip.StopTimes) != len(stops) {
			t.Fatalf("trip %q: expected %d stop times, got %d", trip.Id, len(stops), len(trip.StopTimes))
		}

		for i, s := range stops {
			if trip.StopTimes[i].Stop() != s {
				t.Errorf("trip %q: expected stop %q at position %d, got %q", trip.Id, s.Id, i, trip.StopTimes[i].Stop().Id)
			}

			if trip.StopTimes[i].Sequence() != i+1 {
				t.Errorf("trip %q: sequence of stop time %d changed to %d", trip.Id, i, trip.StopTimes[i].Sequence())
			}
		}
	}
}

// The stops a, b and c are on a line. a and b as well as b and c are duplicates
// (less than 5 m apart), but a and c are not. Depending on the order in which the
// stops are visited, a is merged into b, and b is then merged into c. The stop
// times of a have to follow the whole chain.
func TestStopDuplicateRemoverRewritesStopTimesOfMergeChains(t *testing.T) {
	// the order in which the stops are visited is random, try often enough to see all of them
	for i := 0; i < 100; i++ {
		feed := gtfsparser.NewFeed()

		// 0.000036 degrees latitude are about 4 m. Merging prefers stops with more colons
		// in their id, so a is merged into b, and b is merged into c
		a := &gtfs.Stop{Id: "a", Name: "Hbf", Lat: 48.200000, Lon: 16.37}
		b := &gtfs.Stop{Id: "b:1", Name: "Hbf", Lat: 48.200036, Lon: 16.37}
		c := &gtfs.Stop{Id: "c:1:1", Name: "Hbf", Lat: 48.200072, Lon: 16.37}

		for _, s := range []*gtfs.Stop{a, b, c} {
			feed.Stops[s.Id] = s
		}

		sdrAddTrip(feed, "ta", a)
		tb := sdrAddTrip(feed, "tb", b)
		tc := sdrAddTrip(feed, "tc", c)

		StopDuplicateRemover{DistThresholdStop: 5.0, DistThresholdStation: 50}.Run(feed)

		if _, ok := feed.Stops[b.Id]; ok {
			t.Fatalf("run %d: %q should have been merged into %q", i, b.Id, c.Id)
		}

		sdrAssertStopTimesInFeed(t, feed)

		if tb.StopTimes[0].Stop() != c || tc.StopTimes[0].Stop() != c {
			t.Fatalf("run %d: expected the stop times of %q and %q to use %q", i, b.Id, c.Id, c.Id)
		}
	}
}

// Merging stops must not need memory that grows with the number of stop times.
func TestStopDuplicateRemoverStopTimesMemory(t *testing.T) {
	// use a single chunk for the parallel search, so its (small) allocations don't
	// depend on the number of CPUs
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	feed := gtfsparser.NewFeed()
	stops := make([]*gtfs.Stop, 0)

	// 10 stops, each one exists twice
	for i := 0; i < 10; i++ {
		for _, suffix := range []string{"a", "b"} {
			s := &gtfs.Stop{Id: fmt.Sprintf("%d%s", i, suffix), Name: fmt.Sprintf("Stop %d", i), Lat: 48.0 + float32(i)*0.01, Lon: 16.0}
			feed.Stops[s.Id] = s
			stops = append(stops, s)
		}
	}

	// 400k stop times
	for i := 0; i < 20000; i++ {
		trip := make([]*gtfs.Stop, 20)
		for j := range trip {
			trip[j] = stops[(i+j)%len(stops)]
		}
		sdrAddTrip(feed, fmt.Sprintf("trip%d", i), trip...)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	StopDuplicateRemover{DistThresholdStop: 5.0, DistThresholdStation: 50}.Run(feed)

	runtime.ReadMemStats(&after)

	if len(feed.Stops) != 10 {
		t.Fatalf("expected the duplicate stops to be merged, got %d stops", len(feed.Stops))
	}

	sdrAssertStopTimesInFeed(t, feed)

	// 400k stop times, so a single pointer per stop time would already be 3 MiB
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("merging stops allocated %d KiB for 400000 stop times", allocated>>10)

	if allocated > 1<<20 {
		t.Errorf("merging stops allocated %d KiB for 400000 stop times, expected less than 1024 KiB", allocated>>10)
	}
}
