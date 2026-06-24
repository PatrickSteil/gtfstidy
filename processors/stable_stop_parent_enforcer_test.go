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
		"Hauptbahnhof Süd":  "hauptbahnhof~sud",
		"Münchner Freiheit": "munchner~freiheit",
		"Straße":            "strasse",
		"Düsseldorf":        "dusseldorf",
		"Köln Hbf":          "koln~hbf",
	}
	for in, want := range cases {
		got := normalizeStopName(in)
		if got != want {
			t.Errorf("normalizeStopName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeStopName_NoLeadingTrailingTilde(t *testing.T) {
	got := normalizeStopName("!!!Hauptbahnhof!!!")
	if strings.HasPrefix(got, "~") || strings.HasSuffix(got, "~") {
		t.Errorf("normalizeStopName produced leading/trailing tilde: %q", got)
	}
}

// --- empty / degenerate names -----------------------------------------------

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
	if !strings.HasPrefix(id, "s-") {
		t.Errorf("stableStopID with empty name = %q, expected prefix 's-'", id)
	}
	if !strings.Contains(id, unnamedStopPlaceholder) {
		t.Errorf("stableStopID with empty name = %q, expected to contain %q", id, unnamedStopPlaceholder)
	}
	// Must not contain an empty segment (e.g. "s--name" or "s-geohash-")
	if strings.Contains(id, "--") || strings.HasSuffix(id, "-") {
		t.Errorf("stableStopID with empty name = %q, contains empty segment", id)
	}
}

// --- ß handling -------------------------------------------------------------

func TestTransliterateToASCII_SharpS(t *testing.T) {
	got := transliterateToASCII("Straße")
	if got != "Strasse" {
		t.Errorf("transliterateToASCII(%q) = %q, want %q", "Straße", got, "Strasse")
	}
}

func TestTransliterateToASCII_NoUnrelatedSSCorruption(t *testing.T) {
	got := transliterateToASCII("SS Mannschaft")
	if got != "SS Mannschaft" {
		t.Errorf("transliterateToASCII(%q) = %q, want unchanged %q", "SS Mannschaft", got, "SS Mannschaft")
	}
}

// --- Onestop ID format ------------------------------------------------------

func TestStableStopID_Format(t *testing.T) {
	id := stableStopID("", "Hauptbahnhof Süd", 52.5, 13.4)
	// Must be "s-<geohash>-<name>"
	if !strings.HasPrefix(id, "s-") {
		t.Errorf("stableStopID = %q, want prefix 's-'", id)
	}
	parts := strings.SplitN(id, "-", 3)
	if len(parts) != 3 {
		t.Fatalf("stableStopID = %q, want 3 dash-separated components, got %d", id, len(parts))
	}
	if parts[0] != "s" {
		t.Errorf("entity type = %q, want 's'", parts[0])
	}
	if len(parts[1]) == 0 {
		t.Errorf("geohash component is empty in %q", id)
	}
	if len(parts[2]) == 0 {
		t.Errorf("name component is empty in %q", id)
	}
}

func TestStableStopID_WithDataSource(t *testing.T) {
	id := stableStopID("de-hvv", "Hauptbahnhof", 52.5, 13.4)
	// Name component should be "de-hvv~hauptbahnhof"
	if !strings.HasSuffix(id, "-de-hvv~hauptbahnhof") {
		t.Errorf("stableStopID with dataSource = %q, want suffix '-de-hvv~hauptbahnhof'", id)
	}
}

func TestStableStopID_NameUsesTildes(t *testing.T) {
	id := stableStopID("", "Hauptbahnhof Süd", 52.5, 13.4)
	// Extract name component (everything after second "-")
	idx := strings.Index(id[2:], "-") // skip "s-"
	if idx == -1 {
		t.Fatalf("no name component in %q", id)
	}
	namePart := id[2+idx+1:]
	if strings.Contains(namePart, " ") {
		t.Errorf("name component %q contains spaces", namePart)
	}
	// Word break should be "~", not "-"
	if !strings.Contains(namePart, "~") {
		t.Errorf("name component %q does not use '~' as word separator", namePart)
	}
}

// --- fallback (invalid coordinates) ----------------------------------------

func TestStableStopID_FallbackOnNaN(t *testing.T) {
	nan := float32(math.NaN())
	id := stableStopID("", "Some Stop", nan, nan)
	// Two-component fallback: "s-<name>" (no geohash segment)
	if !strings.HasPrefix(id, "s-") {
		t.Errorf("fallback ID = %q, want prefix 's-'", id)
	}
	// Should be exactly two dash-separated parts: "s" and the name
	parts := strings.SplitN(id, "-", 3)
	if len(parts) != 2 {
		t.Errorf("fallback ID = %q, want exactly 2 dash-separated components (no geohash), got %d", id, len(parts))
	}
}

func TestFallbackStableID_WithDataSource(t *testing.T) {
	id := fallbackStableID("de-hvv", "some~stop", 0, 0)
	if id != "s-de-hvv~some~stop" {
		t.Errorf("fallbackStableID = %q, want %q", id, "s-de-hvv~some~stop")
	}
}

func TestFallbackStableID_WithoutDataSource(t *testing.T) {
	id := fallbackStableID("", "some~stop", 0, 0)
	if id != "s-some~stop" {
		t.Errorf("fallbackStableID = %q, want %q", id, "s-some~stop")
	}
}

// --- truncateToDP (still used by tests, kept in production for clarity) -----

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

// --- safeParentID -----------------------------------------------------------

func TestSafeParentID_ReturnsStableKeyWhenFree(t *testing.T) {
	feed := newFeed()
	got := safeParentID(feed, "s-u33dc1d-hauptbahnhof")
	if got != "s-u33dc1d-hauptbahnhof" {
		t.Errorf("safeParentID = %q, want %q", got, "s-u33dc1d-hauptbahnhof")
	}
}

func TestSafeParentID_ReturnsStableKeyWhenOccupiedByStation(t *testing.T) {
	feed := newFeed()
	addStop(feed, "s-u33dc1d-hauptbahnhof", "Existing Station", 0, 0, 1)
	got := safeParentID(feed, "s-u33dc1d-hauptbahnhof")
	if got != "s-u33dc1d-hauptbahnhof" {
		t.Errorf("safeParentID = %q, want %q (stations are reusable parents)", got, "s-u33dc1d-hauptbahnhof")
	}
}

func TestSafeParentID_AppendsSuffixWhenOccupiedByNonStation(t *testing.T) {
	feed := newFeed()
	addStop(feed, "s-u33dc1d-hauptbahnhof", "A Regular Stop", 0, 0, 0)
	got := safeParentID(feed, "s-u33dc1d-hauptbahnhof")
	if got != "s-u33dc1d-hauptbahnhof~1" {
		t.Errorf("safeParentID = %q, want %q", got, "s-u33dc1d-hauptbahnhof~1")
	}
}

func TestSafeParentID_PanicsWhenExhausted(t *testing.T) {
	feed := newFeed()
	key := "s-u33dc1d-hauptbahnhof"
	addStop(feed, key, "Stop", 0, 0, 0)
	for i := 1; i <= maxParentIDAttempts; i++ {
		addStop(feed, key+"~"+itoa(i), "Stop", 0, 0, 0)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected safeParentID to panic after exhausting %d attempts, it did not", maxParentIDAttempts)
		}
	}()
	safeParentID(feed, key)
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

// --- determinism ------------------------------------------------------------

func TestRun_DeterministicAcrossRuns(t *testing.T) {
	buildFeed := func() *gtfsparser.Feed {
		feed := newFeed()
		addStop(feed, "a", "Central Station", 52.5251, 13.3694, 0)
		addStop(feed, "b", "Central Station", 52.5251, 13.3694, 0)
		addStop(feed, "c", "Central Station", 52.5251, 13.3695, 0)
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

// --- end-to-end smoke tests -------------------------------------------------

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
	if !strings.HasPrefix(child.Parent_station.Id, "s-") {
		t.Errorf("generated parent ID does not start with 's-': %q", child.Parent_station.Id)
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

func TestRun_GeneratedIDMatchesOnestopScheme(t *testing.T) {
	feed := newFeed()
	addStop(feed, "platform-1", "Hauptbahnhof", 52.5, 13.4, 0)

	StableStopParentEnforcer{}.Run(feed)

	id := feed.Stops["platform-1"].Parent_station.Id
	parts := strings.SplitN(id, "-", 3)
	if len(parts) != 3 || parts[0] != "s" {
		t.Errorf("generated ID %q does not follow Onestop scheme 's-<geohash>-<name>'", id)
	}
	// Geohash component should be exactly geohashPrecision characters
	if len(parts[1]) != geohashPrecision {
		t.Errorf("geohash in %q has length %d, want %d", id, len(parts[1]), geohashPrecision)
	}
}
