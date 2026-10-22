// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// Two stations at the same place with the same name are merged by the
// reclusterer. If both have a transfer to the same stop, the transfer of the
// removed station must not stay around.
func TestStopReclustererCollapsingTransfers(t *testing.T) {
	feed := gtfsparser.NewFeed()

	p1 := &gtfs.Stop{Id: "p1", Name: "Hauptbahnhof", Lat: 48.2, Lon: 16.37, Location_type: 1}
	p2 := &gtfs.Stop{Id: "p2", Name: "Hauptbahnhof", Lat: 48.2, Lon: 16.37, Location_type: 1}
	c1 := &gtfs.Stop{Id: "c1", Name: "Hauptbahnhof", Lat: 48.2, Lon: 16.37, Parent_station: p1}
	c2 := &gtfs.Stop{Id: "c2", Name: "Hauptbahnhof", Lat: 48.2, Lon: 16.37, Parent_station: p2}
	x := &gtfs.Stop{Id: "x", Name: "Westbahnhof", Lat: 48.19, Lon: 16.33}

	// a stop far away to the south west, so that the stops above are not in the
	// first row / column of the grid
	z := &gtfs.Stop{Id: "z", Name: "Anchor", Lat: 46.0, Lon: 14.0}

	for _, s := range []*gtfs.Stop{p1, p2, c1, c2, x, z} {
		feed.Stops[s.Id] = s
	}

	feed.Transfers[gtfs.TransferKey{From_stop: p1, To_stop: x}] = gtfs.TransferVal{Transfer_type: 2, Min_transfer_time: 60}
	feed.Transfers[gtfs.TransferKey{From_stop: p2, To_stop: x}] = gtfs.TransferVal{Transfer_type: 2, Min_transfer_time: 120}

	StopReclusterer{DistThreshold: 75, NameSimiThreshold: 0.55, GridCellSize: 10000}.Run(feed)

	if len(feed.Stops) != 5 {
		t.Fatalf("expected the two stations to be merged, got %d stops", len(feed.Stops))
	}

	assertNoDanglingTransfers(t, feed)

	if len(feed.Transfers) != 1 {
		t.Errorf("expected the two transfers to be merged into one, got %d", len(feed.Transfers))
	}
}
