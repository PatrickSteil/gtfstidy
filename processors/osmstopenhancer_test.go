package processors

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// haversineM
// ---------------------------------------------------------------------------

func TestHaversineM_SamePoint(t *testing.T) {
	if d := haversineM(48.0, 8.0, 48.0, 8.0); d != 0 {
		t.Errorf("same point: expected 0, got %f", d)
	}
}

func TestHaversineM_KnownDistance(t *testing.T) {
	// Heidelberg Hbf → Mannheim Hbf, roughly 18 km.
	d := haversineM(49.4037, 8.6757, 49.4794, 8.4694)
	if math.Abs(d-17_600) > 500 {
		t.Errorf("Heidelberg→Mannheim: expected ~17600 m, got %.0f", d)
	}
}

func TestHaversineM_Symmetry(t *testing.T) {
	a := haversineM(48.0, 8.0, 49.0, 9.0)
	b := haversineM(49.0, 9.0, 48.0, 8.0)
	if math.Abs(a-b) > 1e-6 {
		t.Errorf("haversine not symmetric: %f vs %f", a, b)
	}
}

// ---------------------------------------------------------------------------
// degreesForMeters
// ---------------------------------------------------------------------------

func TestDegreesForMeters(t *testing.T) {
	// 111_320 m ≈ 1 degree.
	d := degreesForMeters(48.0, 111_320)
	if math.Abs(d-1.0) > 0.01 {
		t.Errorf("expected ~1.0 degree, got %f", d)
	}
	// 150 m should be a small positive number.
	d150 := degreesForMeters(48.0, 150)
	if d150 <= 0 || d150 > 0.01 {
		t.Errorf("150 m in degrees out of range: %f", d150)
	}
}

// ---------------------------------------------------------------------------
// normaliseString
// ---------------------------------------------------------------------------

func TestNormaliseString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Zürich HB", "zürich hb"},
		{"St. Gallen", "st gallen"},
		{"RAPPERSWIL-JONA", "rapperswiljona"},
		{"  spaces  ", "spaces"},
		{"", ""},
	}
	for _, c := range cases {
		if got := normaliseString(c.in); got != c.want {
			t.Errorf("normalise(%q): want %q, got %q", c.in, c.want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// trigramSimilarity
// ---------------------------------------------------------------------------

func TestTrigramSimilarity_Identity(t *testing.T) {
	if s := trigramSimilarity("Heidelberg Hbf", "Heidelberg Hbf"); math.Abs(s-1.0) > 1e-9 {
		t.Errorf("identical: expected 1.0, got %f", s)
	}
}

func TestTrigramSimilarity_Empty(t *testing.T) {
	if s := trigramSimilarity("", "anything"); s != 0 {
		t.Errorf("empty a: expected 0, got %f", s)
	}
	if s := trigramSimilarity("anything", ""); s != 0 {
		t.Errorf("empty b: expected 0, got %f", s)
	}
}

func TestTrigramSimilarity_Symmetry(t *testing.T) {
	a := trigramSimilarity("Freiburg im Breisgau", "Freiburg Hbf")
	b := trigramSimilarity("Freiburg Hbf", "Freiburg im Breisgau")
	if math.Abs(a-b) > 1e-9 {
		t.Errorf("not symmetric: %.6f vs %.6f", a, b)
	}
}

func TestTrigramSimilarity_Ordering(t *testing.T) {
	close := trigramSimilarity("Hauptbahnhof", "Hauptbahnhof")
	far := trigramSimilarity("Hauptbahnhof", "Hbf")
	if close <= far {
		t.Errorf("expected close (%.3f) > far (%.3f)", close, far)
	}
}

func TestTrigramSimilarity_ShortString(t *testing.T) {
	s := trigramSimilarity("A", "A")
	if s < 0 || s > 1 {
		t.Errorf("short string score out of range: %f", s)
	}
}

// ---------------------------------------------------------------------------
// k-d tree: build and radius search
// ---------------------------------------------------------------------------

func makeStops(items ...osmStop) []osmStop { return items }

func TestKDTree_Empty(t *testing.T) {
	tree := buildKDTree(nil)
	results := tree.radiusSearch(48.0, 8.0, 1.0)
	if len(results) != 0 {
		t.Errorf("empty tree: expected no results, got %d", len(results))
	}
}

func TestKDTree_SingleNode_Hit(t *testing.T) {
	stops := makeStops(osmStop{lat: 48.0, lon: 8.0, name: "A"})
	tree := buildKDTree(stops)
	results := tree.radiusSearch(48.0, 8.0, 0.01)
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestKDTree_SingleNode_Miss(t *testing.T) {
	stops := makeStops(osmStop{lat: 48.0, lon: 8.0, name: "A"})
	tree := buildKDTree(stops)
	// Query far away — radius 0.001° ≈ 110 m, point is ~111 km away.
	results := tree.radiusSearch(49.0, 8.0, 0.001)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestKDTree_ReturnsAllInRadius(t *testing.T) {
	stops := makeStops(
		osmStop{lat: 48.000, lon: 8.000, name: "A"}, // in radius
		osmStop{lat: 48.001, lon: 8.000, name: "B"}, // in radius (~111 m)
		osmStop{lat: 48.010, lon: 8.000, name: "C"}, // out (~1110 m)
		osmStop{lat: 48.000, lon: 8.001, name: "D"}, // in radius (~78 m at 48°)
	)
	tree := buildKDTree(stops)
	// radius 0.002° ≈ 222 m — should include A, B, D but not C.
	results := tree.radiusSearch(48.000, 8.000, 0.002)
	if len(results) != 3 {
		names := make([]string, len(results))
		for i, r := range results {
			names[i] = r.name
		}
		t.Errorf("expected 3 results in radius, got %d: %v", len(results), names)
	}
}

func TestKDTree_LargeSet_AllRetrieved(t *testing.T) {
	// Build 1000 nodes in a tight cluster and verify all are found.
	const N = 1000
	stops := make([]osmStop, N)
	for i := range stops {
		stops[i] = osmStop{
			lat:  48.0 + float64(i%10)*0.0001,
			lon:  8.0 + float64(i/10)*0.0001,
			name: "Stop",
		}
	}
	tree := buildKDTree(stops)
	// Large radius — 1° ≈ 111 km, should catch everything.
	results := tree.radiusSearch(48.0005, 8.0005, 1.0)
	if len(results) != N {
		t.Errorf("expected %d results, got %d", N, len(results))
	}
}

func TestKDTree_NoFalsePositives(t *testing.T) {
	// All nodes are outside the radius; none should be returned.
	stops := makeStops(
		osmStop{lat: 50.0, lon: 10.0},
		osmStop{lat: 51.0, lon: 11.0},
		osmStop{lat: 52.0, lon: 12.0},
	)
	tree := buildKDTree(stops)
	results := tree.radiusSearch(48.0, 8.0, 0.01) // 0.01° ≈ 1.1 km
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

// ---------------------------------------------------------------------------
// bestMatchKD
// ---------------------------------------------------------------------------

func TestBestMatchKD_ExactHit(t *testing.T) {
	stops := makeStops(
		osmStop{lat: 49.40, lon: 8.67, name: "Heidelberg Hbf", ref: "1"},
		osmStop{lat: 48.00, lon: 7.00, name: "Freiburg Hbf", ref: "2"},
	)
	tree := buildKDTree(stops)
	match, score := bestMatchKD(49.40, 8.67, "Heidelberg Hbf", tree, 150, 0.1)
	if match == nil {
		t.Fatal("expected a match, got nil")
	}
	if match.ref != "1" {
		t.Errorf("expected ref 1, got %q", match.ref)
	}
	if score < 0.9 {
		t.Errorf("expected high score, got %.3f", score)
	}
}

func TestBestMatchKD_BeyondMaxDist(t *testing.T) {
	// Node is 200 m away, maxDist is 150 m.
	stops := makeStops(osmStop{lat: 49.40 + 0.0018, lon: 8.67, name: "Far Stop"})
	tree := buildKDTree(stops)
	match, _ := bestMatchKD(49.40, 8.67, "Far Stop", tree, 150, 0.1)
	if match != nil {
		t.Errorf("expected nil match beyond maxDist, got %+v", match)
	}
}

func TestBestMatchKD_BelowMinScore(t *testing.T) {
	// Close by distance but name is completely different.
	stops := makeStops(osmStop{lat: 49.40, lon: 8.67, name: "xyz"})
	tree := buildKDTree(stops)
	// minScore=0.9 — won't be reached when names don't match.
	match, _ := bestMatchKD(49.40, 8.67, "Hauptbahnhof", tree, 150, 0.9)
	if match != nil {
		t.Errorf("expected nil match below minScore, got %+v", match)
	}
}

func TestBestMatchKD_PicksCloser(t *testing.T) {
	stops := makeStops(
		osmStop{lat: 49.40 + 0.0005, lon: 8.67, name: "Hbf", ref: "near"},
		osmStop{lat: 49.40 + 0.0012, lon: 8.67, name: "Hbf", ref: "far"},
	)
	tree := buildKDTree(stops)
	match, _ := bestMatchKD(49.40, 8.67, "Hbf", tree, 500, 0.1)
	if match == nil {
		t.Fatal("expected a match")
	}
	if match.ref != "near" {
		t.Errorf("expected nearer candidate, got ref=%q", match.ref)
	}
}

func TestBestMatchKD_NoCandidates(t *testing.T) {
	tree := buildKDTree(nil)
	match, _ := bestMatchKD(49.40, 8.67, "Anywhere", tree, 150, 0.25)
	if match != nil {
		t.Errorf("expected nil for empty tree, got %+v", match)
	}
}

// ---------------------------------------------------------------------------
// osmWheelchair / amenityNote / isTransitNode / firstTag — unchanged from v1
// ---------------------------------------------------------------------------

func TestOsmWheelchair(t *testing.T) {
	cases := []struct {
		in   string
		want int8
	}{
		{"yes", 1}, {"designated", 1}, {"limited", 1},
		{"no", 2},
		{"", 0}, {"unknown", 0},
	}
	for _, c := range cases {
		if got := osmWheelchair(c.in); got != c.want {
			t.Errorf("osmWheelchair(%q): want %d, got %d", c.in, c.want, got)
		}
	}
}

func TestAmenityNote_AllSet(t *testing.T) {
	m := &osmStop{shelter: "yes", bench: "no", tactile: "yes"}
	want := "shelter=yes bench=no tactile_paving=yes"
	if got := amenityNote(m); got != want {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestAmenityNote_Empty(t *testing.T) {
	if got := amenityNote(&osmStop{}); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestIsTransitNode(t *testing.T) {
	yes := []map[string]string{
		{"public_transport": "stop_position"},
		{"public_transport": "platform"},
		{"highway": "bus_stop"},
		{"railway": "halt"},
		{"railway": "tram_stop"},
		{"railway": "stop"},
	}
	for _, tags := range yes {
		if !isTransitNode(tags) {
			t.Errorf("expected transit node for %v", tags)
		}
	}
	no := []map[string]string{
		{"amenity": "cafe"},
		{"highway": "traffic_signals"},
		{"public_transport": "station"},
		{},
	}
	for _, tags := range no {
		if isTransitNode(tags) {
			t.Errorf("expected non-transit for %v", tags)
		}
	}
}

func TestFirstTag(t *testing.T) {
	tags := map[string]string{"ref": "3", "local_ref": "A"}
	if got := firstTag(tags, "local_ref", "ref"); got != "A" {
		t.Errorf("expected A, got %q", got)
	}
	tags2 := map[string]string{"ref": "3"}
	if got := firstTag(tags2, "local_ref", "ref"); got != "3" {
		t.Errorf("expected 3, got %q", got)
	}
	if got := firstTag(tags2, "nonexistent"); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// applyToStop (via stub — same pattern as v1)
// ---------------------------------------------------------------------------

type stubStop struct {
	Platform_code       string
	Wheelchair_boarding int8
	Desc                string
}

func applyToStopStub(stop *stubStop, m *osmStop) {
	if m.ref != "" && stop.Platform_code == "" {
		stop.Platform_code = m.ref
	}
	if stop.Wheelchair_boarding == 0 {
		if wb := osmWheelchair(m.wheelchair); wb != 0 {
			stop.Wheelchair_boarding = wb
		}
	}
	if stop.Desc == "" {
		if note := amenityNote(m); note != "" {
			stop.Desc = note
		}
	}
}

func TestApplyToStop_FillsEmpty(t *testing.T) {
	stop := &stubStop{}
	applyToStopStub(stop, &osmStop{ref: "A3", wheelchair: "yes", shelter: "yes", bench: "no"})
	if stop.Platform_code != "A3" {
		t.Errorf("Platform_code: want A3, got %q", stop.Platform_code)
	}
	if stop.Wheelchair_boarding != 1 {
		t.Errorf("Wheelchair_boarding: want 1, got %d", stop.Wheelchair_boarding)
	}
	if stop.Desc == "" {
		t.Error("Desc should be set")
	}
}

func TestApplyToStop_DoesNotOverwrite(t *testing.T) {
	stop := &stubStop{Platform_code: "existing", Wheelchair_boarding: 2, Desc: "agency note"}
	applyToStopStub(stop, &osmStop{ref: "new", wheelchair: "yes", shelter: "yes"})
	if stop.Platform_code != "existing" {
		t.Error("Platform_code should not be overwritten")
	}
	if stop.Wheelchair_boarding != 2 {
		t.Error("Wheelchair_boarding should not be overwritten")
	}
	if stop.Desc != "agency note" {
		t.Error("Desc should not be overwritten")
	}
}
