package processors

import (
	"testing"
)

type City struct {
	Name string
	Pop  int
}

func TestKDTreeSearchRange(t *testing.T) {
	cities := []Point[City]{
		{Lat: 52.52, Lon: 13.405, Data: City{"Berlin", 3500000}},
		{Lat: 48.8566, Lon: 2.3522, Data: City{"Paris", 2140000}},
		{Lat: 51.5074, Lon: -0.1278, Data: City{"London", 8900000}},
		{Lat: 40.7128, Lon: -74.0060, Data: City{"New York", 8400000}},
		{Lat: 52.3667, Lon: 4.8945, Data: City{"Amsterdam", 820000}},
		{Lat: 35.6895, Lon: 139.6917, Data: City{"Tokyo", 13960000}},
	}

	// Build the KD-tree
	var root *Node[City]
	for _, city := range cities {
		root = Insert(root, city, 0)
	}

	tests := []struct {
		name     string
		queryLat float64
		queryLon float64
		radiusKm float64
		expected map[string]bool
	}{
		{
			name:     "Near Paris (500 km)",
			queryLat: 48.8566, queryLon: 2.3522, radiusKm: 500,
			expected: map[string]bool{
				"Paris":     true,
				"Amsterdam": true,
				"London":    true,
			},
		},
		{
			name:     "Near Berlin (1000 km)",
			queryLat: 52.52, queryLon: 13.405, radiusKm: 1000,
			expected: map[string]bool{
				"Berlin":    true,
				"Paris":     true,
				"Amsterdam": true,
				"London":    true,
			},
		},
		{
			name:     "Near New York (100 km)",
			queryLat: 40.7128, queryLon: -74.0060, radiusKm: 100,
			expected: map[string]bool{
				"New York": true,
			},
		},
		{
			name:     "Empty result (middle of ocean)",
			queryLat: 0, queryLon: 0, radiusKm: 100,
			expected: map[string]bool{},
		},
		{
			name:     "Self match only (Tokyo, 1 km)",
			queryLat: 35.6895, queryLon: 139.6917, radiusKm: 1,
			expected: map[string]bool{
				"Tokyo": true,
			},
		},
		{
			name:     "Whole world (20000 km)",
			queryLat: 0, queryLon: 0, radiusKm: 20000,
			expected: map[string]bool{
				"Berlin":    true,
				"Paris":     true,
				"London":    true,
				"New York":  true,
				"Amsterdam": true,
				"Tokyo":     true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := Point[City]{Lat: test.queryLat, Lon: test.queryLon}
			var results []Point[City]
			SearchRange(root, query, test.radiusKm, 0, &results)

			found := map[string]bool{}
			for _, pt := range results {
				dist := Haversine(query.Lat, query.Lon, pt.Lat, pt.Lon)
				if dist > test.radiusKm+1e-6 {
					t.Errorf("Returned city %s is %.2f km away (outside radius %.2f)", pt.Data.Name, dist, test.radiusKm)
				}
				found[pt.Data.Name] = true
			}

			// Check all expected cities are found
			for want := range test.expected {
				if !found[want] {
					t.Errorf("Expected city %q not found in results", want)
				}
			}

			// Optionally: warn if there are unexpected results
			for got := range found {
				if !test.expected[got] {
					t.Errorf("Unexpected city %q found", got)
				}
			}
		})
	}
}

func TestBuildKDTreeAndSearchRange(t *testing.T) {
	points := []Point[string]{
		{Lat: 52.370216, Lon: 4.895168, Data: "Amsterdam"},
		{Lat: 48.856613, Lon: 2.352222, Data: "Paris"},
		{Lat: 51.507351, Lon: -0.127758, Data: "London"},
		{Lat: 40.712776, Lon: -74.005974, Data: "New York"},
	}

	tree := BuildKDTree(points, 0)

	// Query near London with a radius of 350 km
	query := Point[string]{Lat: 51.507351, Lon: -0.127758}
	var results []Point[string]

	SearchRange(tree, query, 360, 0, &results)

	expected := map[string]bool{
		"London":    true,
		"Amsterdam": true,
		"Paris":     true, // ~340 km from London, should be included
	}

	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for _, p := range results {
		if !expected[p.Data] {
			t.Errorf("Unexpected point found: %s", p.Data)
		}
		delete(expected, p.Data)
	}

	for missing := range expected {
		t.Errorf("Expected point not found: %s", missing)
	}
}
