// Copyright 2026 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"github.com/uber/h3-go/v4"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// StableStopParentEnforcer ensures that every stop (location_type=0) without an
// existing parent gets a parent station whose stop_id is a stable, content-
// derived ID based on the stop's normalised name and approximate location:
//
//	"1:<normalizedName>:<h3cell>"
//
// When a DataSource tag is set the format is:
//
//	"1:<dataSource>:<normalizedName>:<h3cell>"
//
// If the stop's coordinates cannot be resolved to an H3 cell (e.g. invalid
// lat/lng), a lower-precision degraded fallback is used instead:
//
//	"1:<dataSource>:<normalizedName>:<lat4dp>:<lon4dp>"
//
// This follows the stable-public-transport-ids convention
// (https://github.com/derhuerst/stable-public-transport-ids).
//
// Stops that share the same stable ID are grouped under one parent station,
// so semantically equivalent stops across the feed automatically converge to
// the same parent.  Already existing parent relations are preserved.
type StableStopParentEnforcer struct {
	// DataSource is an optional short tag prepended to every generated ID to
	// scope it to a specific data provider (e.g. "de-hvv").
	// When empty the prefix is omitted.
	DataSource string
}

// unnamedStopPlaceholder is substituted for the name segment of a stable ID
// when a stop has no usable name (empty, or only punctuation/whitespace),
// so the resulting ID stays self-explanatory instead of containing an empty
// field (e.g. "1::8a1fb46622dffff").
const unnamedStopPlaceholder = "unnamed"

// maxParentIDAttempts bounds the linear-probing loop in safeParentID. GTFS
// feeds are finite, so collisions exhaust quickly in practice; this cap just
// turns a hypothetical infinite loop into a clear failure instead of a hang.
const maxParentIDAttempts = 10000

var nonAlphaNumRe = regexp.MustCompile(`[^a-z0-9]+`)

// transliterateToASCII decomposes Unicode via NFD and drops combining marks,
// mapping e.g. ä→a, ö→o, ü→u. ß has no NFD form that yields "ss", so it is
// handled explicitly beforehand. (Uppercase ẞ is folded to lowercase by the
// caller before this function is reached, so only lowercase "ß" needs to be
// special-cased here.)
func transliterateToASCII(s string) string {
	s = strings.ReplaceAll(s, "ß", "ss")
	result, _, _ := transform.String(
		transform.Chain(norm.NFD, transform.RemoveFunc(func(r rune) bool {
			return unicode.Is(unicode.Mn, r)
		})),
		s,
	)
	return result
}

// normalizeStopName returns a lower-cased, hyphenated, ASCII-only version of
// name: Unicode characters are first transliterated to their ASCII base
// letters (ä→a, ö→o, ü→u, ß→ss), then any run of non-alphanumeric characters
// is collapsed to a single "-", and leading/trailing hyphens are trimmed.
//
// Using "-" instead of a space avoids producing whitespace inside the
// colon-delimited stable ID. If the result is empty (the name was empty or
// contained no alphanumeric characters at all), unnamedStopPlaceholder is
// returned instead, so the ID never has an empty field.
func normalizeStopName(name string) string {
	normalized := strings.Trim(
		nonAlphaNumRe.ReplaceAllString(strings.ToLower(transliterateToASCII(name)), "-"),
		"-",
	)
	if normalized == "" {
		return unnamedStopPlaceholder
	}
	return normalized
}

func stableStopID(dataSource, name string, lat, lon float32) string {
	normalized := normalizeStopName(name)

	// Guard against NaN/Inf coordinates — h3.LatLngToCell does not return an
	// error for these; it silently produces a nonsense cell or formats "NaN".
	if math.IsNaN(float64(lat)) || math.IsNaN(float64(lon)) ||
		math.IsInf(float64(lat), 0) || math.IsInf(float64(lon), 0) {
		return fallbackStableID(dataSource, normalized, lat, lon)
	}

	cell, err := h3.LatLngToCell(
		h3.LatLng{
			Lat: float64(lat),
			Lng: float64(lon),
		},
		9,
	)

	if err != nil {
		return fallbackStableID(dataSource, normalized, lat, lon)
	}

	if dataSource != "" {
		return fmt.Sprintf(
			"1:%s:%s:%s",
			dataSource,
			normalized,
			h3.Cell(cell).String(),
		)
	}

	return fmt.Sprintf(
		"1:%s:%s",
		normalized,
		h3.Cell(cell).String(),
	)
}

// fallbackStableID builds the degraded, lower-precision stable ID used when
// the stop's coordinates cannot be resolved to an H3 cell (e.g. invalid
// lat/lng). It uses 4 decimal places (~11m precision at the equator) rather
// than 2dp (~1km) to keep nearby-but-distinct stops from silently colliding
// in this rare path. Split out from stableStopID so it can be exercised
// directly in tests without depending on h3's internal error conditions.
func fallbackStableID(dataSource, normalized string, lat, lon float32) string {
	var latStr, lonStr string
	if math.IsNaN(float64(lat)) || math.IsInf(float64(lat), 0) {
		latStr = fmt.Sprintf("%.4f", 0.0) // or use a sentinel like "0.0000"
	} else {
		latStr = strconv.FormatFloat(truncateToDP(float64(lat), 4), 'f', 4, 64)
	}
	if math.IsNaN(float64(lon)) || math.IsInf(float64(lon), 0) {
		lonStr = fmt.Sprintf("%.4f", 0.0)
	} else {
		lonStr = strconv.FormatFloat(truncateToDP(float64(lon), 4), 'f', 4, 64)
	}
	if dataSource != "" {
		return "1:" + dataSource + ":" + normalized + ":" + latStr + ":" + lonStr
	}
	return "1:" + normalized + ":" + latStr + ":" + lonStr
}

// truncateToDP truncates v toward zero to dp decimal places.
func truncateToDP(v float64, dp int) float64 {
	scale := math.Pow10(dp)
	return math.Trunc(v*scale) / scale
}

// safeParentID returns a feed-unique stop_id for stableKey.
// If stableKey is already taken by a non-station stop it appends ":1", ":2" …
// up to maxParentIDAttempts, after which it panics rather than looping
// forever — this should be unreachable for any real-world GTFS feed.
func safeParentID(feed *gtfsparser.Feed, stableKey string) string {
	if existing, ok := feed.Stops[stableKey]; !ok || existing.Location_type == 1 {
		return stableKey
	}
	for i := 1; i <= maxParentIDAttempts; i++ {
		candidate := stableKey + ":" + strconv.Itoa(i)
		if existing, ok := feed.Stops[candidate]; !ok || existing.Location_type == 1 {
			return candidate
		}
	}
	panic(fmt.Sprintf(
		"safeParentID: exhausted %d candidate IDs for stable key %q; this indicates a pathological feed or a bug",
		maxParentIDAttempts, stableKey,
	))
}

// sortedStopIDs returns the IDs of feed.Stops in deterministic (sorted)
// order. Go map iteration order is randomized per-run, so iterating the map
// directly would make the "winner" of stable-key collisions (and therefore
// which stops get renamed/merged) non-deterministic across runs on identical
// input — undesirable for a processor whose entire purpose is producing
// *stable* IDs.
func sortedStopIDs(feed *gtfsparser.Feed) []string {
	ids := make([]string, 0, len(feed.Stops))
	for id := range feed.Stops {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Run executes the StableStopParentEnforcer on the given feed.
func (e StableStopParentEnforcer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Adding stable parent stations to parentless stops... ")

	keyToParent := make(map[string]*gtfs.Stop)
	for _, id := range sortedStopIDs(feed) {
		s := feed.Stops[id]
		if s.Location_type == 1 {
			keyToParent[s.Id] = s
		}
	}

	created := 0
	renamed := 0

	for _, id := range sortedStopIDs(feed) {
		s := feed.Stops[id]

		if s.Location_type != 0 || s.Parent_station != nil {
			continue
		}

		stableKey := stableStopID(e.DataSource, s.Name, s.Lat, s.Lon)

		parent, exists := keyToParent[stableKey]
		if !exists {
			parentID := safeParentID(feed, stableKey)

			newParent := *s
			newParent.Id = parentID
			newParent.Location_type = 1
			newParent.Parent_station = nil

			feed.Stops[newParent.Id] = &newParent
			parent = feed.Stops[newParent.Id]

			keyToParent[stableKey] = parent
			created++
		}

		s.Parent_station = parent
	}

	// Build a reverse index once so the collision re-pointing below is O(children)
	// rather than O(all stops) per losing station.
	childrenOf := make(map[*gtfs.Stop][]*gtfs.Stop)
	for _, id := range sortedStopIDs(feed) {
		s := feed.Stops[id]
		if s.Parent_station != nil {
			childrenOf[s.Parent_station] = append(childrenOf[s.Parent_station], s)
		}
	}

	// Second pass: give every existing parent station a stable ID if it does
	// not already have one. We derive the stable key from the station's own
	// name and position.
	//
	// Collision rule: if the stable key is already occupied by another station,
	// that other station wins — the current one keeps its original ID (it is
	// already stable enough to stay as-is, and its children remain attached).
	// Iteration is over a fixed, sorted snapshot of stations taken up front
	// (rather than the live map) since this loop mutates feed.Stops as it
	// renames/deletes entries.
	stationSnapshot := make([]*gtfs.Stop, 0)
	for _, id := range sortedStopIDs(feed) {
		s := feed.Stops[id]
		if s.Location_type == 1 {
			stationSnapshot = append(stationSnapshot, s)
		}
	}

	for _, s := range stationSnapshot {
		// The station may have already been removed earlier in this loop
		// (as the losing side of a prior collision) — skip if so.
		if feed.Stops[s.Id] != s {
			continue
		}

		stableKey := stableStopID(e.DataSource, s.Name, s.Lat, s.Lon)

		if s.Id == stableKey {
			continue
		}

		winner, taken := feed.Stops[stableKey]
		if taken && winner != s {
			// Another station already owns that stable key — it wins.
			// Re-point children using the reverse index — O(children of s),
			// not O(all stops).
			for _, child := range childrenOf[s] {
				child.Parent_station = winner
				childrenOf[winner] = append(childrenOf[winner], child)
			}
			delete(childrenOf, s)
			delete(feed.Stops, s.Id)
			continue
		}

		// The stable key is free (or points to s itself via a prior iteration):
		// rename s to the stable ID.
		delete(feed.Stops, s.Id)
		s.Id = stableKey
		feed.Stops[stableKey] = s
		renamed++
	}

	fmt.Fprintf(os.Stdout, "done. (+%d new stations, %d renamed to stable IDs)\n", created, renamed)
}
