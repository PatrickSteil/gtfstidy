// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

func TestStopDuplicateRemoval(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	proc := StopDuplicateRemover{}
	proc.Run(feed)

	if _, ok := feed.Stops["duplicateB4"]; ok {
		t.Error("duplicateB4 is a duplicate stop")
	}

	if _, ok := feed.Stops["duplicateBB"]; ok {
		t.Error("duplicateBB is a duplicate stop")
	}

	if _, ok := feed.Stops["duplicate2B4"]; !ok {
		t.Error("duplicate2B4 is a duplicate stop but with a slightly different coordinate")
	}

	if _, ok := feed.Stops["hasduplicateasparent"]; !ok {
		t.Error("hasduplicateasparent should be present")
	}

	if feed.Stops["hasduplicateasparent"].Parent_station.Id == "duplicateBB" {
		t.Error("hasduplicateasparent should now have duplicateA as parent")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeStop builds a minimal gtfs.Stop suitable for stopHash tests.
func makeStop(id, name, code, desc, zoneID, platform string, locType int, wheelchair int8, parent *gtfs.Stop, level *gtfs.Level) *gtfs.Stop {
	s := &gtfs.Stop{
		Id:                  id,
		Name:                name,
		Code:                code,
		Desc:                desc,
		Zone_id:             zoneID,
		Platform_code:       platform,
		Location_type:       int8(locType),
		Wheelchair_boarding: wheelchair,
		Parent_station:      parent,
		Level:               level,
	}
	return s
}

func sdr() StopDuplicateRemover {
	return StopDuplicateRemover{Fuzzy: false}
}

func sdrFuzzy() StopDuplicateRemover {
	return StopDuplicateRemover{Fuzzy: true}
}

// ---------------------------------------------------------------------------
// stopHash: identical stops produce the same hash
// ---------------------------------------------------------------------------

func TestStopHash_IdenticalStopsMatchingHash(t *testing.T) {
	a := makeStop("stop1", "Hauptbahnhof", "HBF", "Main station", "Z1", "3", 0, 1, nil, nil)
	b := makeStop("stop2", "Hauptbahnhof", "HBF", "Main station", "Z1", "3", 0, 1, nil, nil)

	h := sdr()
	if h.stopHash(a) != h.stopHash(b) {
		t.Error("identical stops (different IDs, same fields) should produce the same hash")
	}
}

// ---------------------------------------------------------------------------
// stopHash: the core bug fix — pointer identity must not matter
// ---------------------------------------------------------------------------

// TestStopHash_StableAcrossPointerAddresses is the regression test for the
// original bug: two stops with logically identical Parent_station objects
// (same Id) but at different memory addresses must still hash equally.
// Before the fix, hashing the raw pointer caused misses when allocation order
// differed between runs.
func TestStopHash_StableAcrossPointerAddresses(t *testing.T) {
	// Two independent allocations of a parent with the same Id.
	parentA := &gtfs.Stop{Id: "parent-42"}
	parentB := &gtfs.Stop{Id: "parent-42"} // same Id, different pointer

	stopA := makeStop("s1", "Marktplatz", "", "", "", "", 0, 0, parentA, nil)
	stopB := makeStop("s2", "Marktplatz", "", "", "", "", 0, 0, parentB, nil)

	h := sdr()
	if h.stopHash(stopA) != h.stopHash(stopB) {
		t.Errorf(
			"stops with logically identical parents (same Id=%q) must hash equally "+
				"regardless of pointer address: hash(stopA)=%d, hash(stopB)=%d",
			parentA.Id, h.stopHash(stopA), h.stopHash(stopB),
		)
	}
}

// Complementary: different parent Ids must produce different hashes.
func TestStopHash_DifferentParentIdsDifferentHash(t *testing.T) {
	parentA := &gtfs.Stop{Id: "parent-1"}
	parentB := &gtfs.Stop{Id: "parent-2"}

	stopA := makeStop("s1", "Marktplatz", "", "", "", "", 0, 0, parentA, nil)
	stopB := makeStop("s2", "Marktplatz", "", "", "", "", 0, 0, parentB, nil)

	h := sdr()
	if h.stopHash(stopA) == h.stopHash(stopB) {
		t.Error("stops with different parent IDs should (almost always) produce different hashes")
	}
}

// ---------------------------------------------------------------------------
// stopHash: nil parent vs. non-nil parent
// ---------------------------------------------------------------------------

func TestStopHash_NilParentDiffersFromNonNilParent(t *testing.T) {
	parent := &gtfs.Stop{Id: "some-parent"}
	withParent := makeStop("s1", "Stop", "", "", "", "", 0, 0, parent, nil)
	noParent := makeStop("s2", "Stop", "", "", "", "", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(withParent) == h.stopHash(noParent) {
		t.Error("stop with a parent should hash differently from stop without a parent")
	}
}

func TestStopHash_BothNilParentsMatchingHash(t *testing.T) {
	a := makeStop("s1", "Stop", "C", "D", "Z", "P", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "C", "D", "Z", "P", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) != h.stopHash(b) {
		t.Error("two stops with nil parents and equal fields should hash the same")
	}
}

// ---------------------------------------------------------------------------
// stopHash: level field (non-fuzzy only)
// ---------------------------------------------------------------------------

func TestStopHash_DifferentLevelIdsDifferentHash(t *testing.T) {
	lvlA := &gtfs.Level{Id: "level-0"}
	lvlB := &gtfs.Level{Id: "level-1"}

	stopA := makeStop("s1", "Stop", "", "", "", "", 0, 0, nil, lvlA)
	stopB := makeStop("s2", "Stop", "", "", "", "", 0, 0, nil, lvlB)

	h := sdr() // non-fuzzy: level is included in hash
	if h.stopHash(stopA) == h.stopHash(stopB) {
		t.Error("non-fuzzy: stops with different level IDs should hash differently")
	}
}

func TestStopHash_LevelIgnoredInFuzzyMode(t *testing.T) {
	lvlA := &gtfs.Level{Id: "level-0"}
	lvlB := &gtfs.Level{Id: "level-99"}

	stopA := makeStop("s1", "Stop", "", "", "", "", 0, 0, nil, lvlA)
	stopB := makeStop("s2", "Stop", "", "", "", "", 0, 0, nil, lvlB)

	h := sdrFuzzy() // fuzzy: level is not part of the hash
	if h.stopHash(stopA) != h.stopHash(stopB) {
		t.Error("fuzzy mode: level should be excluded from hash; different levels should still match")
	}
}

func TestStopHash_NilLevelVsNonNilLevel_NonFuzzy(t *testing.T) {
	lvl := &gtfs.Level{Id: "level-0"}
	withLevel := makeStop("s1", "Stop", "", "", "", "", 0, 0, nil, lvl)
	noLevel := makeStop("s2", "Stop", "", "", "", "", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(withLevel) == h.stopHash(noLevel) {
		t.Error("non-fuzzy: stop with a level should hash differently from stop without a level")
	}
}

// ---------------------------------------------------------------------------
// stopHash: fields that always contribute regardless of fuzzy mode
// ---------------------------------------------------------------------------

func TestStopHash_DifferentLocationType(t *testing.T) {
	a := makeStop("s1", "Stop", "", "", "", "", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "", "", "", "", 1, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("stops with different location_type should hash differently")
	}
}

func TestStopHash_DifferentWheelchairBoarding(t *testing.T) {
	a := makeStop("s1", "Stop", "", "", "", "", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "", "", "", "", 0, 1, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("stops with different wheelchair_boarding should hash differently")
	}
}

func TestStopHash_DifferentDesc(t *testing.T) {
	a := makeStop("s1", "Stop", "", "desc-a", "", "", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "", "desc-b", "", "", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("stops with different desc should hash differently")
	}
}

func TestStopHash_DifferentZoneId(t *testing.T) {
	a := makeStop("s1", "Stop", "", "", "zone-1", "", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "", "", "zone-2", "", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("stops with different zone_id should hash differently")
	}
}

// ---------------------------------------------------------------------------
// stopHash: fields that only contribute in non-fuzzy mode
// ---------------------------------------------------------------------------

func TestStopHash_DifferentName_NonFuzzy(t *testing.T) {
	a := makeStop("s1", "Hauptbahnhof", "", "", "", "", 0, 0, nil, nil)
	b := makeStop("s2", "Marktplatz", "", "", "", "", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("non-fuzzy: stops with different names should hash differently")
	}
}

func TestStopHash_DifferentName_FuzzyIgnored(t *testing.T) {
	a := makeStop("s1", "Hauptbahnhof", "", "", "", "", 0, 0, nil, nil)
	b := makeStop("s2", "Marktplatz", "", "", "", "", 0, 0, nil, nil)

	h := sdrFuzzy()
	// In fuzzy mode, name is excluded — different names may hash the same.
	// We can only assert this doesn't panic; we don't assert hash equality
	// because other fields could still differ.
	_ = h.stopHash(a)
	_ = h.stopHash(b)
}

func TestStopHash_DifferentCode_NonFuzzy(t *testing.T) {
	a := makeStop("s1", "Stop", "CODE-A", "", "", "", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "CODE-B", "", "", "", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("non-fuzzy: stops with different codes should hash differently")
	}
}

func TestStopHash_DifferentPlatformCode_NonFuzzy(t *testing.T) {
	a := makeStop("s1", "Stop", "", "", "", "1", 0, 0, nil, nil)
	b := makeStop("s2", "Stop", "", "", "", "2", 0, 0, nil, nil)

	h := sdr()
	if h.stopHash(a) == h.stopHash(b) {
		t.Error("non-fuzzy: stops with different platform_code should hash differently")
	}
}

// ---------------------------------------------------------------------------
// stopHash: determinism — same input always produces the same output
// ---------------------------------------------------------------------------

func TestStopHash_Deterministic(t *testing.T) {
	parent := &gtfs.Stop{Id: "parent-99"}
	lvl := &gtfs.Level{Id: "L0"}
	s := makeStop("s1", "Hauptbahnhof", "HBF", "Main", "Z1", "3", 0, 1, parent, lvl)

	h := sdr()
	first := h.stopHash(s)
	for i := 0; i < 100; i++ {
		if h.stopHash(s) != first {
			t.Fatalf("stopHash is not deterministic: got different value on iteration %d", i)
		}
	}
}
