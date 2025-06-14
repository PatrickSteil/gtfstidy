package processors

import (
	"math"
	"testing"
)

// Helper to compare floats
func almostEqual(a, b float64) bool {
	const eps = 1e-6
	return math.Abs(a-b) < eps
}

// Simple payload type for tests
type Payload struct {
	ID string
}

// Create sample data
func testPoints() []Point[Payload] {
	return []Point[Payload]{
		{Lat: 48.1371, Lon: 11.5754, Data: Payload{"Munich"}},
		{Lat: 52.5200, Lon: 13.4050, Data: Payload{"Berlin"}},
		{Lat: 50.1109, Lon: 8.6821, Data: Payload{"Frankfurt"}},
		{Lat: 53.5511, Lon: 9.9937, Data: Payload{"Hamburg"}},
		{Lat: 51.1657, Lon: 10.4515, Data: Payload{"Germany Center"}},
	}
}

// Brute-force search to compare against KD-tree result
func linearSearch(points []Point[Payload], query Point[Payload], radiusKm float64) []Point[Payload] {
	var result []Point[Payload]
	for _, p := range points {
		if Haversine(p.Lat, p.Lon, query.Lat, query.Lon) <= radiusKm {
			result = append(result, p)
		}
	}
	return result
}

// Check that all points in 'a' are in 'b' (by ID)
func matchResults(t *testing.T, a, b []Point[Payload]) {
	t.Helper()
	if len(a) != len(b) {
		t.Errorf("Expected %d results, got %d", len(a), len(b))
		return
	}
	found := map[string]bool{}
	for _, p := range b {
		found[p.Data.ID] = true
	}
	for _, p := range a {
		if !found[p.Data.ID] {
			t.Errorf("Missing point in KD result: %v", p.Data.ID)
		}
	}
}

// Test building the tree and searching
func TestBuildAndSearch(t *testing.T) {
	points := testPoints()
	tree := BuildKDTree(points, 0)
	if tree == nil {
		t.Fatal("Tree is nil after building from points")
	}

	query := Point[Payload]{Lat: 50.0, Lon: 10.0}
	radius := 300.0 // km

	var kdResults []Point[Payload]
	SearchRange(tree, query, radius, 0, &kdResults)

	linear := linearSearch(points, query, radius)
	matchResults(t, kdResults, linear)
}

// Test inserting into the tree
func TestInsert(t *testing.T) {
	points := testPoints()
	tree := BuildKDTree(points[:3], 0) // smaller initial tree

	insertPoint := Point[Payload]{Lat: 53.5511, Lon: 9.9937, Data: Payload{"Hamburg"}}
	tree = Insert(tree, insertPoint, 0)

	query := insertPoint
	var results []Point[Payload]
	SearchRange(tree, query, 50, 0, &results)

	found := false
	for _, p := range results {
		if p.Data.ID == "Hamburg" {
			found = true
		}
	}
	if !found {
		t.Errorf("Inserted point not found in search")
	}
}

// Test edge case: empty input
func TestEmptyTree(t *testing.T) {
	var empty []Point[Payload]
	tree := BuildKDTree(empty, 0)
	if tree != nil {
		t.Errorf("Expected nil tree for empty input, got non-nil")
	}
}

// Test bounding box correctness
func TestLatLonBoundingBox(t *testing.T) {
	minLat, maxLat, minLon, maxLon := latLonBoundingBox(50, 8, 100)
	if minLat >= maxLat || minLon >= maxLon {
		t.Errorf("Invalid bounding box: min >= max")
	}
}

// Test Haversine accuracy on known values
func TestHaversine(t *testing.T) {
	// Munich to Berlin
	lat1, lon1 := 48.1371, 11.5754
	lat2, lon2 := 52.5200, 13.4050
	dist := Haversine(lat1, lon1, lat2, lon2)
	if dist < 500 || dist > 600 {
		t.Errorf("Unexpected Haversine distance: %.2f km", dist)
	}
}

// Test all points returned within large radius
func TestAllPointsReturned(t *testing.T) {
	points := testPoints()
	tree := BuildKDTree(points, 0)

	var results []Point[Payload]
	SearchRange(tree, Point[Payload]{Lat: 51.0, Lon: 10.0}, 10000, 0, &results)

	if len(results) != len(points) {
		t.Errorf("Expected all %d points, got %d", len(points), len(results))
	}
}
