package processors

import (
	"math"
	"testing"

	"github.com/patrickbr/gtfsparser/gtfs"
)

// ---------------------------------------------------------------------------
// haversineM
// ---------------------------------------------------------------------------

func TestHaversineM_ZeroDistance(t *testing.T) {
	d := haversineM(48.0, 8.0, 48.0, 8.0)
	if d != 0 {
		t.Errorf("expected 0 distance for identical points, got %f", d)
	}
}

func TestHaversineM_KnownDistance(t *testing.T) {
	// Roughly 1 degree of latitude ~ 111.32 km.
	d := haversineM(0, 0, 1, 0)
	want := 111_320.0
	tol := 1500.0 // generous tolerance for the spherical approximation
	if math.Abs(d-want) > tol {
		t.Errorf("expected ~%f m, got %f m", want, d)
	}
}

func TestHaversineM_Symmetric(t *testing.T) {
	d1 := haversineM(48.1, 8.1, 48.2, 8.3)
	d2 := haversineM(48.2, 8.3, 48.1, 8.1)
	if math.Abs(d1-d2) > 1e-9 {
		t.Errorf("expected symmetric distance, got %f vs %f", d1, d2)
	}
}

// ---------------------------------------------------------------------------
// degreesForMetersLat / degreesForMetersLon
// ---------------------------------------------------------------------------

func TestDegreesForMetersLat(t *testing.T) {
	got := degreesForMetersLat(111_320)
	if math.Abs(got-1.0) > 1e-9 {
		t.Errorf("expected ~1.0 degree, got %f", got)
	}
}

func TestDegreesForMetersLon_ShrinksWithLatitude(t *testing.T) {
	atEquator := degreesForMetersLon(0, 150)
	atHighLat := degreesForMetersLon(60, 150)
	if atHighLat <= atEquator {
		t.Errorf("expected longitude degree-radius to grow with latitude (equator=%f, 60deg=%f)",
			atEquator, atHighLat)
	}
}

func TestDegreesForMetersLon_PoleClamped(t *testing.T) {
	// Should not blow up (divide by ~0) near the pole.
	got := degreesForMetersLon(89.999, 150)
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Errorf("expected finite value near pole, got %f", got)
	}
}

// ---------------------------------------------------------------------------
// normaliseString
// ---------------------------------------------------------------------------

func TestNormaliseString_LowercasesAndFoldsDiacritics(t *testing.T) {
	cases := map[string]string{
		"Müller":         "muller",
		"Straße":         "strasse",
		"Café René":      "cafe rene",
		"  Hauptbahnhof": "hauptbahnhof",
		"":               "",
	}
	for in, want := range cases {
		got := normaliseString(in)
		if got != want {
			t.Errorf("normaliseString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormaliseString_StripsPunctuation(t *testing.T) {
	got := normaliseString("Bahnhof, Platz/Eingang!")
	want := "bahnhof platzeingang"
	if got != want {
		t.Errorf("normaliseString(...) = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// trigramSet / trigramSimilarity / trigramSimilaritySets
// ---------------------------------------------------------------------------

func TestTrigramSimilarity_IdenticalStrings(t *testing.T) {
	got := trigramSimilarity("Hauptbahnhof", "Hauptbahnhof")
	if math.Abs(got-1.0) > 1e-9 {
		t.Errorf("expected similarity 1.0 for identical strings, got %f", got)
	}
}

func TestTrigramSimilarity_EmptyStrings(t *testing.T) {
	if got := trigramSimilarity("", "Hauptbahnhof"); got != 0 {
		t.Errorf("expected 0 similarity when one string is empty, got %f", got)
	}
	if got := trigramSimilarity("", ""); got != 0 {
		t.Errorf("expected 0 similarity when both strings are empty, got %f", got)
	}
}

func TestTrigramSimilarity_CompletelyDifferent(t *testing.T) {
	got := trigramSimilarity("aaa", "zzz")
	if got != 0 {
		t.Errorf("expected 0 similarity for disjoint trigram sets, got %f", got)
	}
}

func TestTrigramSimilarity_DiacriticInsensitive(t *testing.T) {
	got := trigramSimilarity("Müller Straße", "Muller Strasse")
	if got < 0.5 {
		t.Errorf("expected high similarity after diacritic folding, got %f", got)
	}
}

func TestTrigramSimilaritySets_MatchesStringVersion(t *testing.T) {
	a, b := "Marktplatz", "Marktplatz Nord"
	want := trigramSimilarity(a, b)
	got := trigramSimilaritySets(trigramSet(a), trigramSet(b))
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("trigramSimilaritySets = %f, want %f (from trigramSimilarity)", got, want)
	}
}

func TestTrigramSimilaritySets_EmptySet(t *testing.T) {
	got := trigramSimilaritySets(map[string]struct{}{}, trigramSet("Hauptbahnhof"))
	if got != 0 {
		t.Errorf("expected 0 for an empty set, got %f", got)
	}
}

func TestTrigramSet_ShortStringPadding(t *testing.T) {
	// Strings shorter than 3 runes should still produce at least one trigram
	// rather than an empty/invalid set.
	set := trigramSet("ab")
	if len(set) == 0 {
		t.Errorf("expected at least one trigram for a short string, got empty set")
	}
}

// ---------------------------------------------------------------------------
// osmWheelchair
// ---------------------------------------------------------------------------

func TestOsmWheelchair(t *testing.T) {
	cases := map[string]int8{
		"yes":        1,
		"designated": 1,
		"limited":    1,
		"no":         2,
		"":           0,
		"unknown":    0,
	}
	for in, want := range cases {
		if got := osmWheelchair(in); got != want {
			t.Errorf("osmWheelchair(%q) = %d, want %d", in, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// amenityNote
// ---------------------------------------------------------------------------

func TestAmenityNote_AllFieldsPresent(t *testing.T) {
	m := &osmStop{shelter: "yes", bench: "no", tactile: "yes"}
	got := amenityNote(m)
	want := "shelter=yes bench=no tactile_paving=yes"
	if got != want {
		t.Errorf("amenityNote() = %q, want %q", got, want)
	}
}

func TestAmenityNote_PartialFields(t *testing.T) {
	m := &osmStop{shelter: "yes"}
	got := amenityNote(m)
	want := "shelter=yes"
	if got != want {
		t.Errorf("amenityNote() = %q, want %q", got, want)
	}
}

func TestAmenityNote_NoFields(t *testing.T) {
	m := &osmStop{}
	if got := amenityNote(m); got != "" {
		t.Errorf("amenityNote() = %q, want empty string", got)
	}
}

// ---------------------------------------------------------------------------
// applyToStop
// ---------------------------------------------------------------------------

func TestApplyToStop_FillsEmptyFieldsOnly(t *testing.T) {
	stop := &gtfs.Stop{
		Platform_code:       "",
		Wheelchair_boarding: 0,
		Desc:                "",
	}
	m := &osmStop{
		ref:        "3",
		wheelchair: "yes",
		shelter:    "yes",
	}

	res := applyToStop(stop, m, 100, false, 30)

	if stop.Platform_code != "3" {
		t.Errorf("expected Platform_code to be set to '3', got %q", stop.Platform_code)
	}
	if stop.Wheelchair_boarding != 1 {
		t.Errorf("expected Wheelchair_boarding=1, got %d", stop.Wheelchair_boarding)
	}
	if stop.Desc != "shelter=yes" {
		t.Errorf("expected Desc='shelter=yes', got %q", stop.Desc)
	}
	if !res.platformSet || !res.wheelchairSet || !res.descSet {
		t.Errorf("expected all three applyResult flags set, got %+v", res)
	}
	if res.coordsFixed {
		t.Errorf("expected coordsFixed=false when FixCoordinates is disabled")
	}
}

func TestApplyToStop_DoesNotOverwriteExistingFields(t *testing.T) {
	stop := &gtfs.Stop{
		Platform_code:       "EXISTING",
		Wheelchair_boarding: 2,
		Desc:                "EXISTING DESC",
	}
	m := &osmStop{
		ref:        "3",
		wheelchair: "yes",
		shelter:    "yes",
	}

	res := applyToStop(stop, m, 100, false, 30)

	if stop.Platform_code != "EXISTING" {
		t.Errorf("expected Platform_code to remain 'EXISTING', got %q", stop.Platform_code)
	}
	if stop.Wheelchair_boarding != 2 {
		t.Errorf("expected Wheelchair_boarding to remain 2, got %d", stop.Wheelchair_boarding)
	}
	if stop.Desc != "EXISTING DESC" {
		t.Errorf("expected Desc to remain unchanged, got %q", stop.Desc)
	}
	if res.platformSet || res.wheelchairSet || res.descSet {
		t.Errorf("expected no fields to be marked as set, got %+v", res)
	}
}

func TestApplyToStop_FixCoordinatesWithinThreshold(t *testing.T) {
	stop := &gtfs.Stop{Lat: 48.0, Lon: 8.0}
	m := &osmStop{lat: 48.0001, lon: 8.0001}

	res := applyToStop(stop, m, 10, true, 30)

	// Compare with a small tolerance: stop.Lat/Lon are float32, so converting
	// back to float64 won't exactly equal the original float64 m.lat/m.lon.
	const tol = 1e-6
	if math.Abs(float64(stop.Lat)-m.lat) > tol || math.Abs(float64(stop.Lon)-m.lon) > tol {
		t.Errorf("expected coordinates to be snapped to OSM node (%f,%f), got (%f,%f)",
			m.lat, m.lon, stop.Lat, stop.Lon)
	}
	if !res.coordsFixed {
		t.Errorf("expected coordsFixed=true")
	}
}

func TestApplyToStop_DoesNotFixCoordinatesBeyondThreshold(t *testing.T) {
	origLat, origLon := float32(48.0), float32(8.0)
	stop := &gtfs.Stop{Lat: origLat, Lon: origLon}
	m := &osmStop{lat: 49.0, lon: 9.0} // far away

	res := applyToStop(stop, m, 5000, true, 30)

	if stop.Lat != origLat || stop.Lon != origLon {
		t.Errorf("expected coordinates to remain unchanged when dist > threshold, got (%f,%f)",
			stop.Lat, stop.Lon)
	}
	if res.coordsFixed {
		t.Errorf("expected coordsFixed=false when dist exceeds threshold")
	}
}

func TestApplyToStop_FixCoordinatesDisabledByDefault(t *testing.T) {
	stop := &gtfs.Stop{Lat: 48.0, Lon: 8.0}
	m := &osmStop{lat: 48.0001, lon: 8.0001}

	res := applyToStop(stop, m, 1, false, 30)

	if stop.Lat != 48.0 || stop.Lon != 8.0 {
		t.Errorf("expected coordinates unchanged when fixCoords=false")
	}
	if res.coordsFixed {
		t.Errorf("expected coordsFixed=false when fixCoords=false")
	}
}

// ---------------------------------------------------------------------------
// k-d tree: buildKDTree / radiusSearch / chooseAxis
// ---------------------------------------------------------------------------

func sampleStops() []osmStop {
	// A small cluster around (48.0, 8.0) plus one far-away outlier.
	return []osmStop{
		{lat: 48.0000, lon: 8.0000, name: "Marktplatz", nameTrig: trigramSet("Marktplatz")},
		{lat: 48.0005, lon: 8.0005, name: "Rathaus", nameTrig: trigramSet("Rathaus")},
		{lat: 48.0010, lon: 8.0010, name: "Hauptbahnhof", nameTrig: trigramSet("Hauptbahnhof")},
		{lat: 48.0020, lon: 8.0020, name: "Universitaet", nameTrig: trigramSet("Universitaet")},
		{lat: 49.5000, lon: 9.5000, name: "FarAway", nameTrig: trigramSet("FarAway")},
	}
}

func TestBuildKDTree_EmptyInput(t *testing.T) {
	tree := buildKDTree(nil)
	if len(tree.nodes) != 0 {
		t.Errorf("expected empty tree for empty input, got %d nodes", len(tree.nodes))
	}
}

func TestBuildKDTree_NodeCountMatchesInput(t *testing.T) {
	stops := sampleStops()
	tree := buildKDTree(stops)
	if len(tree.nodes) != len(stops) {
		t.Errorf("expected %d nodes in tree, got %d", len(stops), len(tree.nodes))
	}
}

func TestRadiusSearch_FindsNearbyOnly(t *testing.T) {
	stops := sampleStops()
	tree := buildKDTree(stops)

	radiusDegLat := degreesForMetersLat(200)
	radiusDegLon := degreesForMetersLon(48.0, 200)

	results := tree.radiusSearch(48.0000, 8.0000, radiusDegLat, radiusDegLon)

	foundFarAway := false
	for _, r := range results {
		if r.name == "FarAway" {
			foundFarAway = true
		}
	}
	if foundFarAway {
		t.Errorf("expected the far-away outlier to be excluded from a 200m radius search")
	}
	if len(results) == 0 {
		t.Errorf("expected at least one nearby candidate, got none")
	}
}

func TestRadiusSearch_EmptyTree(t *testing.T) {
	tree := buildKDTree(nil)
	results := tree.radiusSearch(48.0, 8.0, 0.01, 0.01)
	if results != nil {
		t.Errorf("expected nil results for empty tree, got %v", results)
	}
}

func TestRadiusSearch_ZeroRadiusMatchesExactPointOnly(t *testing.T) {
	stops := sampleStops()
	tree := buildKDTree(stops)

	// A tiny but non-zero radius around an exact node location should find
	// at least that node.
	results := tree.radiusSearch(48.0000, 8.0000, 1e-9, 1e-9)
	if len(results) == 0 {
		t.Errorf("expected to find the exact-match node, got no results")
	}
	for _, r := range results {
		if r.name != "Marktplatz" {
			t.Errorf("expected only the exact-match node, also got %q", r.name)
		}
	}
}

func TestChooseAxis_PicksHigherVarianceAxis(t *testing.T) {
	// Spread mostly along latitude.
	stops := []osmStop{
		{lat: 0.0, lon: 8.0},
		{lat: 1.0, lon: 8.0},
		{lat: 2.0, lon: 8.0},
		{lat: 3.0, lon: 8.0001},
	}
	indices := []int{0, 1, 2, 3}
	axis := chooseAxis(stops, indices)
	if axis != 0 {
		t.Errorf("expected axis=0 (lat) for latitude-dominant spread, got %d", axis)
	}

	// Spread mostly along longitude.
	stops2 := []osmStop{
		{lat: 48.0, lon: 0.0},
		{lat: 48.0001, lon: 1.0},
		{lat: 48.0, lon: 2.0},
		{lat: 48.0001, lon: 3.0},
	}
	axis2 := chooseAxis(stops2, indices)
	if axis2 != 1 {
		t.Errorf("expected axis=1 (lon) for longitude-dominant spread, got %d", axis2)
	}
}

// ---------------------------------------------------------------------------
// bestMatchKD (integration of distance + name scoring)
// ---------------------------------------------------------------------------

func TestBestMatchKD_PicksClosestGoodNameMatch(t *testing.T) {
	stops := sampleStops()
	tree := buildKDTree(stops)

	match, score := bestMatchKD(48.0010, 8.0010, "Hauptbahnhof", tree, 200, 0.25)
	if match == nil {
		t.Fatalf("expected a match, got nil")
	}
	if match.name != "Hauptbahnhof" {
		t.Errorf("expected match 'Hauptbahnhof', got %q", match.name)
	}
	if score <= 0.25 {
		t.Errorf("expected score above MinScore, got %f", score)
	}
}

func TestBestMatchKD_NoMatchBelowMinScore(t *testing.T) {
	stops := sampleStops()
	tree := buildKDTree(stops)

	// Far outside any reasonable radius/name match.
	match, score := bestMatchKD(10.0, 10.0, "Nonexistent", tree, 50, 0.25)
	if match != nil {
		t.Errorf("expected no match, got %q with score %f", match.name, score)
	}
}

func TestBestMatchKD_EmptyTree(t *testing.T) {
	tree := buildKDTree(nil)
	match, _ := bestMatchKD(48.0, 8.0, "Anything", tree, 150, 0.25)
	if match != nil {
		t.Errorf("expected no match against an empty tree, got %q", match.name)
	}
}

// ---------------------------------------------------------------------------
// isTransitNode / firstTag
// ---------------------------------------------------------------------------

func TestIsTransitNode(t *testing.T) {
	cases := []struct {
		tags map[string]string
		want bool
	}{
		{map[string]string{"public_transport": "platform"}, true},
		{map[string]string{"public_transport": "stop_position"}, true},
		{map[string]string{"highway": "bus_stop"}, true},
		{map[string]string{"railway": "tram_stop"}, true},
		{map[string]string{"railway": "station"}, false},
		{map[string]string{"shop": "bakery"}, false},
		{map[string]string{}, false},
	}
	for _, c := range cases {
		if got := isTransitNode(c.tags); got != c.want {
			t.Errorf("isTransitNode(%v) = %v, want %v", c.tags, got, c.want)
		}
	}
}

func TestFirstTag_ReturnsFirstNonEmpty(t *testing.T) {
	tags := map[string]string{"ref": "12"}
	got := firstTag(tags, "local_ref", "ref")
	if got != "12" {
		t.Errorf("firstTag(...) = %q, want %q", got, "12")
	}
}

func TestFirstTag_PrefersEarlierKey(t *testing.T) {
	tags := map[string]string{"local_ref": "A", "ref": "B"}
	got := firstTag(tags, "local_ref", "ref")
	if got != "A" {
		t.Errorf("firstTag(...) = %q, want %q", got, "A")
	}
}

func TestFirstTag_NoMatch(t *testing.T) {
	tags := map[string]string{"foo": "bar"}
	got := firstTag(tags, "local_ref", "ref")
	if got != "" {
		t.Errorf("firstTag(...) = %q, want empty string", got)
	}
}
