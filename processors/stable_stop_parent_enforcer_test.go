// Copyright 2026 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
	"strings"
	"testing"

	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

// --- helpers ---------------------------------------------------------------

func newFeed() *gtfsparser.Feed {
	feed := &gtfsparser.Feed{}
	feed.Stops = make(map[string]*gtfs.Stop)
	return feed
}

func addStop(feed *gtfsparser.Feed, id, name string, lat, lon float32, locationType int8) *gtfs.Stop {
	s := &gtfs.Stop{
		Id:            id,
		Name:          name,
		Lat:           lat,
		Lon:           lon,
		Location_type: locationType,
	}
	feed.Stops[id] = s
	return s
}

// --- normalizeStopName / whitespace -----------------------------------------

func TestNormalizeStopName_NoWhitespace(t *testing.T) {
	cases := []string{
		"Hauptbahnhof Süd",
		"Berlin   Alexanderplatz",
		"Münchner Freiheit",
		"Straße des 17. Juni",
		"Place de la Concorde",
		"  leading and trailing  ",
	}
	for _, name := range cases {
		got := normalizeStopName(name)
		if strings.ContainsAny(got, " \t\n") {
			t.Errorf("normalizeStopName(%q) = %q, contains whitespace", name, got)
		}
	}
}

func TestNormalizeStopName_Transliteration(t *testing.T) {
	cases := map[string]string{
		"Hauptbahnhof Süd":  "hauptbahnhof-sud",
		"Münchner Freiheit": "munchner-freiheit",
		"Straße":            "strasse",
		"Düsseldorf":        "dusseldorf",
		"Köln Hbf":          "koln-hbf",
	}
	for in, want := range cases {
		got := normalizeStopName(in)
		if got != want {
			t.Errorf("normalizeStopName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeStopName_NoLeadingTrailingHyphen(t *testing.T) {
	got := normalizeStopName("!!!Hauptbahnhof!!!")
	if strings.HasPrefix(got, "-") || strings.HasSuffix(got, "-") {
		t.Errorf("normalizeStopName produced leading/trailing hyphen: %q", got)
	}
}

// --- fix #2: empty / degenerate names ---------------------------------------

func TestNormalizeStopName_EmptyName(t *testing.T) {
	for _, name := range []string{"", "   ", "!!!", "---", "..."} {
		got := normalizeStopName(name)
		if got != unnamedStopPlaceholder {
			t.Errorf("normalizeStopName(%q) = %q, want %q", name, got, unnamedStopPlaceholder)
		}
	}
}

func TestStableStopID_EmptyNameProducesUsableID(t *testing.T) {
	id := stableStopID("", "", 52.5, 13.4)
	if !strings.Contains(id, unnamedStopPlaceholder) {
		t.Errorf("stableStopID with empty name = %q, expected to contain %q", id, unnamedStopPlaceholder)
	}
	if strings.Contains(id, "::") {
		t.Errorf("stableStopID with empty name = %q, contains empty field '::'", id)
	}
}

// --- fix #3: ß handling without dead code ------------------------------------

func TestTransliterateToASCII_SharpS(t *testing.T) {
	got := transliterateToASCII("Straße")
	if got != "Strasse" {
		t.Errorf("transliterateToASCII(%q) = %q, want %q", "Straße", got, "Strasse")
	}
}

func TestTransliterateToASCII_NoUnrelatedSSCorruption(t *testing.T) {
	// Literal uppercase "SS" in the input (unrelated to ß) should pass through
	// unchanged by transliterateToASCII; only ß is special-cased.
	got := transliterateToASCII("SS Mannschaft")
	if got != "SS Mannschaft" {
		t.Errorf("transliterateToASCII(%q) = %q, want unchanged %q", "SS Mannschaft", got, "SS Mannschaft")
	}
}

// --- fix #4: degraded fallback precision / format ---------------------------

func TestStableStopID_DegradedFallback_NaNCoords(t *testing.T) {
	// Call fallbackStableID directly — this is what it's exported for in tests.
	id := fallbackStableID("", "Some Stop", 52.1234567, 13.1234567)
	parts := strings.Split(id, ":")
	if len(parts) < 4 {
		t.Fatalf("expected fallback ID with at least 4 colon-separated fields, got %q", id)
	}
	latStr := parts[len(parts)-2]
	lonStr := parts[len(parts)-1]
	for _, numStr := range []string{latStr, lonStr} {
		dotIdx := strings.Index(numStr, ".")
		if dotIdx == -1 {
			t.Errorf("fallback coordinate %q has no decimal point", numStr)
			continue
		}
		decimals := len(numStr) - dotIdx - 1
		if decimals != 4 {
			t.Errorf("fallback coordinate %q has %d decimal places, want 4", numStr, decimals)
		}
	}
}

func TestStableStopID_DegradedFallback_WithDataSource(t *testing.T) {
	nan := float32(math.NaN())
	id := stableStopID("de-hvv", "Some Stop", nan, nan)
	if !strings.HasPrefix(id, "1:de-hvv:some-stop:") {
		t.Errorf("stableStopID with dataSource = %q, want prefix %q", id, "1:de-hvv:some-stop:")
	}
}

func TestTruncateToDP(t *testing.T) {
	cases := []struct {
		in   float64
		dp   int
		want float64
	}{
		{1.23456, 4, 1.2345},
		{-1.23456, 4, -1.2345},
		{52.520008, 4, 52.5200},
		{0, 4, 0},
	}
	for _, c := range cases {
		got := truncateToDP(c.in, c.dp)
		if got != c.want {
			t.Errorf("truncateToDP(%v, %d) = %v, want %v", c.in, c.dp, got, c.want)
		}
	}
}

// --- fix #5: safeParentID bounded loop --------------------------------------

func TestSafeParentID_ReturnsStableKeyWhenFree(t *testing.T) {
	feed := newFeed()
	got := safeParentID(feed, "1:test:abc")
	if got != "1:test:abc" {
		t.Errorf("safeParentID = %q, want %q", got, "1:test:abc")
	}
}

func TestSafeParentID_ReturnsStableKeyWhenOccupiedByStation(t *testing.T) {
	feed := newFeed()
	addStop(feed, "1:test:abc", "Existing Station", 0, 0, 1) // location_type=1
	got := safeParentID(feed, "1:test:abc")
	if got != "1:test:abc" {
		t.Errorf("safeParentID = %q, want %q (stations are reusable parents)", got, "1:test:abc")
	}
}

func TestSafeParentID_AppendsSuffixWhenOccupiedByNonStation(t *testing.T) {
	feed := newFeed()
	addStop(feed, "1:test:abc", "A Regular Stop", 0, 0, 0) // location_type=0
	got := safeParentID(feed, "1:test:abc")
	if got != "1:test:abc:1" {
		t.Errorf("safeParentID = %q, want %q", got, "1:test:abc:1")
	}
}

func TestSafeParentID_PanicsWhenExhausted(t *testing.T) {
	feed := newFeed()
	addStop(feed, "1:test:abc", "Stop", 0, 0, 0)
	for i := 1; i <= maxParentIDAttempts; i++ {
		addStop(feed, "1:test:abc:"+itoa(i), "Stop", 0, 0, 0)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected safeParentID to panic after exhausting %d attempts, it did not", maxParentIDAttempts)
		}
	}()
	safeParentID(feed, "1:test:abc")
}

// itoa avoids importing strconv twice in the test file for this one helper use.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// --- determinism -------------------------------------------------------------

func TestRun_DeterministicAcrossRuns(t *testing.T) {
	buildFeed := func() *gtfsparser.Feed {
		feed := newFeed()
		addStop(feed, "a", "Central Station", 52.5251, 13.3694, 0)
		addStop(feed, "b", "Central Station", 52.5251, 13.3694, 0)
		addStop(feed, "c", "Central Station", 52.5251, 13.3695, 0) // near-duplicate name/coords
		addStop(feed, "d", "Other Stop", 48.1351, 11.5820, 0)
		return feed
	}

	var results []map[string]string
	for run := 0; run < 5; run++ {
		feed := buildFeed()
		StableStopParentEnforcer{}.Run(feed)

		snapshot := make(map[string]string)
		for _, id := range sortedStopIDs(feed) {
			s := feed.Stops[id]
			if s.Parent_station != nil {
				snapshot[id] = s.Parent_station.Id
			} else {
				snapshot[id] = ""
			}
		}
		results = append(results, snapshot)
	}

	first := results[0]
	for i, r := range results[1:] {
		if len(r) != len(first) {
			t.Fatalf("run %d produced %d stops, run 0 produced %d", i+1, len(r), len(first))
		}
		for id, parent := range first {
			if r[id] != parent {
				t.Errorf("run %d: stop %q has parent %q, run 0 had %q (non-deterministic)", i+1, id, r[id], parent)
			}
		}
	}
}

// --- end-to-end smoke test ---------------------------------------------------

func TestRun_CreatesParentForParentlessStop(t *testing.T) {
	feed := newFeed()
	addStop(feed, "platform-1", "Hauptbahnhof Süd", 52.5, 13.4, 0)

	StableStopParentEnforcer{}.Run(feed)

	child := feed.Stops["platform-1"]
	if child.Parent_station == nil {
		t.Fatal("expected child stop to have a parent station assigned")
	}
	if child.Parent_station.Location_type != 1 {
		t.Errorf("parent station has Location_type = %d, want 1", child.Parent_station.Location_type)
	}
	if strings.ContainsAny(child.Parent_station.Id, " \t\n") {
		t.Errorf("generated parent ID contains whitespace: %q", child.Parent_station.Id)
	}
}

func TestRun_GroupsStopsWithSameStableKeyUnderOneParent(t *testing.T) {
	feed := newFeed()
	addStop(feed, "platform-1", "Hauptbahnhof", 52.5, 13.4, 0)
	addStop(feed, "platform-2", "Hauptbahnhof", 52.5, 13.4, 0)

	StableStopParentEnforcer{}.Run(feed)

	p1 := feed.Stops["platform-1"].Parent_station
	p2 := feed.Stops["platform-2"].Parent_station
	if p1 == nil || p2 == nil {
		t.Fatal("expected both stops to get a parent")
	}
	if p1 != p2 {
		t.Errorf("expected both stops to share the same parent, got %q and %q", p1.Id, p2.Id)
	}
}

func TestRun_PreservesExistingParent(t *testing.T) {
	feed := newFeed()
	parent := addStop(feed, "manual-parent", "Hauptbahnhof", 52.5, 13.4, 1)
	child := addStop(feed, "platform-1", "Hauptbahnhof", 52.5, 13.4, 0)
	child.Parent_station = parent

	StableStopParentEnforcer{}.Run(feed)

	if feed.Stops["platform-1"].Parent_station != parent {
		t.Error("expected existing parent assignment to be preserved")
	}
}
