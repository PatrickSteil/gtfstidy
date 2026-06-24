// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"testing"

	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

// helper to build a single-stop cluster at a given lat/lon
func newTestCluster(id string, lat, lon float32) *StopCluster {
	s := &gtfs.Stop{Id: id, Lat: lat, Lon: lon}
	return NewStopCluster(s)
}

// TestStopClusterIdxFindsCloseNeighbor checks that a broad-phase lookup
// returns clusters that are genuinely close, and excludes ones that are
// far away.
func TestStopClusterIdxFindsCloseNeighbor(t *testing.T) {
	c0 := newTestCluster("s0", 10.0000, 10.0000)
	c1 := newTestCluster("s1", 10.0003, 10.0003) // ~46m from s0
	c2 := newTestCluster("s2", 10.0500, 10.0500) // ~7.7km from s0

	clusters := []*StopCluster{c0, c1, c2}

	idx := NewStopClusterIdx(clusters, 200, 200)

	neighs := idx.GetNeighborsByLatLon(float64(c0.Childs[0].Lat), float64(c0.Childs[0].Lon), 200)

	if !neighs[0] {
		t.Errorf("expected cluster 0 (itself) to be found, got %v", neighs)
	}
	if !neighs[1] {
		t.Errorf("expected cluster 1 (~46m away) to be found within d=200, got %v", neighs)
	}
	if neighs[2] {
		t.Errorf("did not expect cluster 2 (~7.7km away) to be found within d=200, got %v", neighs)
	}
}

// TestStopClusterIdxGetNeighborsExcludesSelf checks that GetNeighbors never
// returns the cluster id that was explicitly excluded, even though that
// cluster's own stop trivially matches its own location.
func TestStopClusterIdxGetNeighborsExcludesSelf(t *testing.T) {
	c0 := newTestCluster("s0", 10.0000, 10.0000)
	c1 := newTestCluster("s1", 10.0003, 10.0003)
	c2 := newTestCluster("s2", 10.0500, 10.0500)

	clusters := []*StopCluster{c0, c1, c2}

	idx := NewStopClusterIdx(clusters, 200, 200)

	neighs := idx.GetNeighbors(0, c0, 200)

	if neighs[0] {
		t.Errorf("expected cluster 0 to be excluded from its own neighbor set, got %v", neighs)
	}
	if !neighs[1] {
		t.Errorf("expected cluster 1 to be a neighbor of cluster 0, got %v", neighs)
	}
	if neighs[2] {
		t.Errorf("did not expect cluster 2 to be a neighbor of cluster 0, got %v", neighs)
	}
}

// TestStopClusterIdxMinCornerRegression is a regression test for a bug where
// GetNeighborsByLatLon computed the lower search-window bound by subtracting
// xPerm/yPerm from an already-clamped value a second time. For any stop
// sitting exactly at the grid's minimum corner (cellX == 0 or cellY == 0),
// any positive search radius leads to an unsigned underflow / over-clamped
// window, and the broad-phase lookup silently returns nothing -- not even
// the cluster the query point itself belongs to.
func TestStopClusterIdxMinCornerRegression(t *testing.T) {
	// lat=0, lon=0 maps to mercator (0, 0), which becomes the grid's exact
	// minimum corner (llx=0, lly=0), i.e. cellX == cellY == 0 for this point.
	c0 := newTestCluster("min-corner", 0.0, 0.0)
	c1 := newTestCluster("far-corner", 0.01, 0.01) // just to give the grid a non-zero extent

	clusters := []*StopCluster{c0, c1}

	idx := NewStopClusterIdx(clusters, 100, 100)

	for _, d := range []float64{1, 50, 100, 250} {
		neighs := idx.GetNeighborsByLatLon(0.0, 0.0, d)
		if !neighs[0] {
			t.Errorf("d=%v: expected the query point's own cluster (0) to be found at the grid's min corner, got %v", d, neighs)
		}
	}
}

// TestStopClusterIdxDegenerateSinglePoint makes sure a feed where the
// bounding box collapses to a single point (one stop, or several
// coincident stops) doesn't panic and degrades gracefully.
func TestStopClusterIdxDegenerateSinglePoint(t *testing.T) {
	c0 := newTestCluster("only", 5.0, 5.0)
	clusters := []*StopCluster{c0}

	idx := NewStopClusterIdx(clusters, 100, 100)

	var neighs map[int]bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("GetNeighborsByLatLon panicked on degenerate single-point index: %v", r)
			}
		}()
		neighs = idx.GetNeighborsByLatLon(5.0, 5.0, 50)
	}()

	if len(neighs) != 0 {
		t.Errorf("expected no neighbors from a degenerate single-point index, got %v", neighs)
	}
}
