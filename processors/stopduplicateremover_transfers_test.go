// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// assertNoDanglingTransfers fails the test for every transfer that references a
// stop that is not part of the feed (anymore).
func assertNoDanglingTransfers(t *testing.T, feed *gtfsparser.Feed) {
	t.Helper()

	id := func(s *gtfs.Stop) string {
		if s == nil {
			return "<nil>"
		}
		return s.Id
	}

	for tk := range feed.Transfers {
		for _, s := range []*gtfs.Stop{tk.From_stop, tk.To_stop} {
			if s != nil && feed.Stops[s.Id] != s {
				t.Errorf("transfer %s -> %s references stop %q, which is not in the feed", id(tk.From_stop), id(tk.To_stop), s.Id)
			}
		}
	}
}

// Two duplicate stops with a transfer to the same third stop: after merging the
// duplicates, both transfers have the same key. The transfer of the removed stop
// must not stay around.
func TestStopDuplicateRemoverCollapsingTransfers(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	b := &gtfs.Stop{Id: "b", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	c := &gtfs.Stop{Id: "c", Name: "Elsewhere", Lat: 48.3, Lon: 16.5}

	for _, s := range []*gtfs.Stop{a, b, c} {
		feed.Stops[s.Id] = s
	}

	feed.Transfers[gtfs.TransferKey{From_stop: a, To_stop: c}] = gtfs.TransferVal{Transfer_type: 2, Min_transfer_time: 60}
	feed.Transfers[gtfs.TransferKey{From_stop: b, To_stop: c}] = gtfs.TransferVal{Transfer_type: 2, Min_transfer_time: 120}

	StopDuplicateRemover{DistThresholdStop: 5.0, DistThresholdStation: 50}.Run(feed)

	if len(feed.Stops) != 2 {
		t.Fatalf("expected the duplicate stops to be merged, got %d stops", len(feed.Stops))
	}

	assertNoDanglingTransfers(t, feed)

	if len(feed.Transfers) != 1 {
		t.Errorf("expected the two transfers to be merged into one, got %d", len(feed.Transfers))
	}
}

// A transfer between two duplicate stops that are both merged into a third one
// (the stop with the smallest id) ends up as a transfer at that stop.
func TestStopDuplicateRemoverTransferBetweenMergedStops(t *testing.T) {
	feed := gtfsparser.NewFeed()

	a := &gtfs.Stop{Id: "a", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	b := &gtfs.Stop{Id: "b", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	c := &gtfs.Stop{Id: "c", Name: "Hbf", Lat: 48.2, Lon: 16.37}
	d := &gtfs.Stop{Id: "d", Name: "Elsewhere", Lat: 48.3, Lon: 16.5}

	for _, s := range []*gtfs.Stop{a, b, c, d} {
		feed.Stops[s.Id] = s
	}

	val := gtfs.TransferVal{Transfer_type: 2, Min_transfer_time: 30}
	feed.Transfers[gtfs.TransferKey{From_stop: b, To_stop: c}] = val

	StopDuplicateRemover{DistThresholdStop: 5.0, DistThresholdStation: 50}.Run(feed)

	if len(feed.Stops) != 2 {
		t.Fatalf("expected the duplicate stops to be merged, got %d stops", len(feed.Stops))
	}

	assertNoDanglingTransfers(t, feed)

	got, ok := feed.Transfers[gtfs.TransferKey{From_stop: a, To_stop: a}]
	if !ok || len(feed.Transfers) != 1 {
		t.Fatalf("expected a single transfer at the remaining stop %q, got %d transfers", a.Id, len(feed.Transfers))
	}

	if got != val {
		t.Errorf("transfer value changed from %v to %v", val, got)
	}
}
