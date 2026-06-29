// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
	"testing"

	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeStop(id string, lat, lon float32) *gtfs.Stop {
	return &gtfs.Stop{Id: id, Lat: lat, Lon: lon}
}

func makeStopNaN(id string, parent *gtfs.Stop) *gtfs.Stop {
	return &gtfs.Stop{
		Id:             id,
		Lat:            float32(math.NaN()),
		Lon:            float32(math.NaN()),
		Parent_station: parent,
	}
}

func makeCluster(parents, childs []*gtfs.Stop) *StopCluster {
	return &StopCluster{Parents: parents, Childs: childs}
}

// safeIdx wraps NewStopClusterIdx and fails the test on panic.
func safeIdx(t *testing.T, clusters []*StopCluster, cell float64) *StopClusterIdx {
	t.Helper()
	var idx *StopClusterIdx
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("NewStopClusterIdx panicked unexpectedly: %v", r)
			}
		}()
		idx = NewStopClusterIdx(clusters, cell, cell)
	}()
	return idx
}

// Two stops whose web-mercator coordinates differ in BOTH x and y so that
// xWidth > 0 and yHeight > 0 for any cell size smaller than the separation.
//
//	(48.0, 8.0) → (48.01, 8.01): Δx ≈ 1 113 m, Δy ≈ 1 664 m
const (
	lat1 = float32(48.0)
	lon1 = float32(8.0)
	lat2 = float32(48.01)
	lon2 = float32(8.01)
	// separation is ~1 113 m in x and ~1 664 m in y
	// cell must be < min(Δx, Δy) so both dimensions are > 0
	smallCell = 500.0
	// cell > max(Δx, Δy) puts both stops in the same cell
	bigCell = 5000.0
)

// ---------------------------------------------------------------------------
// FIX 1 – bounding-box used if/else if so a single-point feed (llx == urx)
//          never updated urx, leaving width = +Inf - +Inf = NaN / -Inf.
// ---------------------------------------------------------------------------

// All stops at the same point: bounding box must be degenerate (zero), not
// invalid (negative / NaN).
func TestFix1_SinglePointBBox(t *testing.T) {
	s := makeStop("A", 48.0, 8.0)
	idx := safeIdx(t, []*StopCluster{makeCluster([]*gtfs.Stop{s}, nil)}, bigCell)
	if idx.width < 0 || idx.height < 0 {
		t.Fatalf("single-point feed: expected width/height >= 0, got w=%v h=%v", idx.width, idx.height)
	}
}

// Two stops that share longitude: x-extent is zero, y-extent is non-zero.
func TestFix1_SameLongitudeTwoStops(t *testing.T) {
	s1 := makeStop("A", 48.0, 8.0)
	s2 := makeStop("B", 48.5, 8.0) // same lon, different lat
	idx := safeIdx(t, []*StopCluster{
		makeCluster([]*gtfs.Stop{s1}, nil),
		makeCluster([]*gtfs.Stop{s2}, nil),
	}, bigCell)
	if idx.width < 0 || idx.height < 0 {
		t.Fatalf("same-longitude stops: expected width/height >= 0, got w=%v h=%v", idx.width, idx.height)
	}
}

// Perfect duplicates: both dimensions are zero.
func TestFix1_IdenticalDuplicateStops(t *testing.T) {
	s1 := makeStop("A", 48.0, 8.0)
	s2 := makeStop("B", 48.0, 8.0)
	idx := safeIdx(t, []*StopCluster{
		makeCluster([]*gtfs.Stop{s1}, nil),
		makeCluster([]*gtfs.Stop{s2}, nil),
	}, bigCell)
	if idx.width != 0 || idx.height != 0 {
		t.Fatalf("duplicate stops: expected width=0 height=0, got w=%v h=%v", idx.width, idx.height)
	}
}

// ---------------------------------------------------------------------------
// FIX 2 – neighbor window subtracted xPerm twice, making the search rectangle
//          too narrow and missing legitimate neighbors.
// ---------------------------------------------------------------------------

// Two clusters ~1.9 km apart (2D) with a 500 m cell must find each other
// when queried with a 3 000 m radius (6 cells).
func TestFix2_NeighborWindowCoversNearbyCluster(t *testing.T) {
	s1 := makeStop("S1", lat1, lon1)
	s2 := makeStop("S2", lat2, lon2) // Δx≈1113 m, Δy≈1664 m → ~2000 m apart
	c1 := makeCluster(nil, []*gtfs.Stop{s1})
	c2 := makeCluster(nil, []*gtfs.Stop{s2})
	clusters := []*StopCluster{c1, c2}

	idx := NewStopClusterIdx(clusters, smallCell, smallCell)

	neighs := idx.GetNeighbors(0, c1, 3000)
	if _, found := neighs[1]; !found {
		t.Fatal("FIX2: cluster 1 should be a neighbor of cluster 0 within 3000 m but was not found")
	}
}

// The search must be symmetric: cluster 0 finds cluster 1 AND vice versa.
func TestFix2_NeighborWindowSymmetric(t *testing.T) {
	s1 := makeStop("S1", lat1, lon1)
	s2 := makeStop("S2", lat2, lon2)
	c1 := makeCluster(nil, []*gtfs.Stop{s1})
	c2 := makeCluster(nil, []*gtfs.Stop{s2})
	clusters := []*StopCluster{c1, c2}

	idx := NewStopClusterIdx(clusters, smallCell, smallCell)

	if _, found := idx.GetNeighbors(0, c1, 3000)[1]; !found {
		t.Error("FIX2: cluster 0 did not find cluster 1")
	}
	if _, found := idx.GetNeighbors(1, c2, 3000)[0]; !found {
		t.Error("FIX2: cluster 1 did not find cluster 0")
	}
}

// A stop searched at its own exact coordinates must always find itself.
func TestFix2_ExactCoordAlwaysFindsCluster(t *testing.T) {
	s := makeStop("A", lat1, lon1)
	c := makeCluster(nil, []*gtfs.Stop{s})
	idx := NewStopClusterIdx([]*StopCluster{c}, smallCell, smallCell)

	neighs := idx.GetNeighborsByLatLon(float64(lat1), float64(lon1), 1)
	if _, found := neighs[0]; !found {
		t.Fatal("FIX2: cluster not found when searching at its exact coordinates")
	}
}

// A cluster far outside the radius must NOT appear in results.
func TestFix2_DistantClusterNotReturned(t *testing.T) {
	s1 := makeStop("S1", lat1, lon1)
	s2 := makeStop("S2", 49.0, 9.0) // ~150 km away
	c1 := makeCluster(nil, []*gtfs.Stop{s1})
	c2 := makeCluster(nil, []*gtfs.Stop{s2})
	idx := NewStopClusterIdx([]*StopCluster{c1, c2}, smallCell, smallCell)

	neighs := idx.GetNeighborsByLatLon(float64(lat1), float64(lon1), 100)
	if _, found := neighs[1]; found {
		t.Fatal("FIX2: distant cluster must NOT appear within a 100 m search radius")
	}
}

// Zero-dimension grid (all stops co-located): GetNeighborsByLatLon must not
// panic and may return an empty result (degenerate index).
func TestFix2_ZeroDimensionGridDoesNotPanic(t *testing.T) {
	// Same lat AND lon → Δx = Δy = 0 → xWidth = yHeight = 0 → flat grid len 0.
	// The code must not panic when the grid is empty.
	s1 := makeStop("A", 48.0, 8.0)
	s2 := makeStop("B", 48.0, 8.0)
	c1 := makeCluster(nil, []*gtfs.Stop{s1})
	c2 := makeCluster(nil, []*gtfs.Stop{s2})
	idx := NewStopClusterIdx([]*StopCluster{c1, c2}, smallCell, smallCell)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("FIX2: GetNeighborsByLatLon panicked on zero-dimension grid: %v", r)
			}
		}()
		idx.GetNeighborsByLatLon(48.0, 8.0, 1000)
	}()
}

// ---------------------------------------------------------------------------
// FIX 3 – getStopLatLon was not used consistently for Parents; stops with
//          optional location type have NaN lat/lon and rely on parent_station.
// ---------------------------------------------------------------------------

// A child stop with NaN coords must be indexed at its parent station's coords.
func TestFix3_NaNChildUsesParentCoords(t *testing.T) {
	station := makeStop("station", 49.4093, 8.6942)
	child := makeStopNaN("child", station)

	c := makeCluster(nil, []*gtfs.Stop{child})
	idx := safeIdx(t, []*StopCluster{c}, bigCell)

	neighs := idx.GetNeighborsByLatLon(float64(station.Lat), float64(station.Lon), 1)
	if _, found := neighs[0]; !found {
		t.Fatal("FIX3: child with NaN coords should be indexed at parent station coordinates")
	}
}

// A parent stop (location_type 1) with NaN coords and a grandparent must not
// panic during construction or querying.
func TestFix3_NaNParentUsesGrandparentCoords(t *testing.T) {
	grandparent := makeStop("gp", 49.4093, 8.6942)
	parent := makeStopNaN("par", grandparent)
	parent.Location_type = 1

	c := makeCluster([]*gtfs.Stop{parent}, nil)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("FIX3: panic for NaN parent with grandparent: %v", r)
			}
		}()
		idx := NewStopClusterIdx([]*StopCluster{c}, bigCell, bigCell)
		idx.GetNeighbors(0, c, bigCell*2)
	}()
}

// A stop with NaN coords AND no parent must panic (spec: coords unresolvable).
func TestFix3_NaNStopWithoutParentPanics(t *testing.T) {
	bad := &gtfs.Stop{
		Id:  "bad",
		Lat: float32(math.NaN()),
		Lon: float32(math.NaN()),
	}
	c := makeCluster(nil, []*gtfs.Stop{bad})

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("FIX3: expected panic for NaN stop without parent, but did not panic")
		}
	}()
	NewStopClusterIdx([]*StopCluster{c}, bigCell, bigCell)
}

// ---------------------------------------------------------------------------
// FIX 4 – flat 1D [][]int32 grid replacing [][]map[int]bool.
//          Correctness invariants that must hold regardless of backing store.
// ---------------------------------------------------------------------------

// Five well-separated clusters must all be retrievable in a broad search.
func TestFix4_FlatGridAllClustersRetrievable(t *testing.T) {
	base := [][2]float32{
		{48.00, 8.00}, {48.01, 8.01}, {48.02, 8.02}, {48.03, 8.03}, {48.04, 8.04},
	}
	clusters := make([]*StopCluster, len(base))
	for i, b := range base {
		clusters[i] = makeCluster(nil, []*gtfs.Stop{makeStop("s", b[0], b[1])})
	}

	idx := NewStopClusterIdx(clusters, smallCell, smallCell)

	neighs := idx.GetNeighborsByLatLon(48.02, 8.02, 1_000_000)
	for i := range clusters {
		if _, found := neighs[i]; !found {
			t.Errorf("FIX4: cluster %d not found in broad search", i)
		}
	}
}

// Three stops close enough to share a single cell must all be stored
// (no overwrite in the flat slice).
func TestFix4_MultipleClustersInSameCell(t *testing.T) {
	s1 := makeStop("A", lat1, lon1)
	s2 := makeStop("B", lat2, lon2)
	// third stop: also within the Δx/Δy range but slightly offset
	s3 := makeStop("C", float32((float64(lat1)+float64(lat2))/2), float32((float64(lon1)+float64(lon2))/2))

	c1 := makeCluster(nil, []*gtfs.Stop{s1})
	c2 := makeCluster(nil, []*gtfs.Stop{s2})
	c3 := makeCluster(nil, []*gtfs.Stop{s3})
	clusters := []*StopCluster{c1, c2, c3}

	// bigCell ensures all three land in the same cell
	idx := NewStopClusterIdx(clusters, bigCell, bigCell)

	neighs := idx.GetNeighborsByLatLon(float64(lat1), float64(lon1), 1_000_000)
	for i := 0; i < 3; i++ {
		if _, found := neighs[i]; !found {
			t.Errorf("FIX4: cluster %d missing from same-cell broad search", i)
		}
	}
}

// excludeCid must never appear in GetNeighbors results.
func TestFix4_ExcludeSelfFromNeighbors(t *testing.T) {
	s1 := makeStop("A", lat1, lon1)
	s2 := makeStop("B", lat2, lon2)
	c1 := makeCluster(nil, []*gtfs.Stop{s1})
	c2 := makeCluster(nil, []*gtfs.Stop{s2})
	clusters := []*StopCluster{c1, c2}

	idx := NewStopClusterIdx(clusters, smallCell, smallCell)

	for excludeId, c := range clusters {
		neighs := idx.GetNeighbors(excludeId, c, 1_000_000)
		if _, found := neighs[excludeId]; found {
			t.Errorf("FIX4: cluster %d appears in its own neighbor set", excludeId)
		}
	}
}

// ---------------------------------------------------------------------------
// General edge cases
// ---------------------------------------------------------------------------

func TestEdge_EmptyClusters(t *testing.T) {
	idx := safeIdx(t, []*StopCluster{}, bigCell)
	neighs := idx.GetNeighborsByLatLon(48.0, 8.0, 1000)
	if len(neighs) != 0 {
		t.Fatalf("expected no neighbors for empty index, got %v", neighs)
	}
}

func TestEdge_SingleClusterSingleStop(t *testing.T) {
	s := makeStop("only", lat1, lon1)
	c := makeCluster(nil, []*gtfs.Stop{s})
	idx := NewStopClusterIdx([]*StopCluster{c}, bigCell, bigCell)

	if _, found := idx.GetNeighborsByLatLon(float64(lat1), float64(lon1), 1)[0]; !found {
		t.Fatal("single stop not found at its own coordinates")
	}
	if neighs := idx.GetNeighbors(0, c, 1_000_000); len(neighs) != 0 {
		t.Fatalf("expected no neighbors for single cluster, got %v", neighs)
	}
}

// A cluster with both parents and children: all stops must be indexed.
func TestMixed_ParentsAndChildsIndexed(t *testing.T) {
	parent := makeStop("par", lat1, lon1)
	parent.Location_type = 1
	child1 := makeStop("ch1", lat2, lon2)
	child2 := makeStop("ch2", float32((float64(lat1)+float64(lat2))/2), float32((float64(lon1)+float64(lon2))/2))

	c := makeCluster([]*gtfs.Stop{parent}, []*gtfs.Stop{child1, child2})
	idx := NewStopClusterIdx([]*StopCluster{c}, smallCell, smallCell)

	for _, s := range []*gtfs.Stop{parent, child1, child2} {
		if _, found := idx.GetNeighborsByLatLon(float64(s.Lat), float64(s.Lon), 1)[0]; !found {
			t.Errorf("stop %s not indexed in cluster 0", s.Id)
		}
	}
}
