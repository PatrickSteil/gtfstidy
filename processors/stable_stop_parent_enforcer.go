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

	"github.com/mmcloughlin/geohash"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// StableStopParentEnforcer ensures that every stop (location_type=0) without an
// existing parent gets a parent station whose stop_id is a stable, content-
// derived Onestop ID based on the stop's normalised name and approximate location:
//
//	"s-<geohash>-<normalizedName>"
//
// When a DataSource tag is set the name component is prefixed with it:
//
//	"s-<geohash>-<dataSource>~<normalizedName>"
//
// This follows the Transitland Onestop ID scheme
// (https://www.transit.land/documentation/concepts/onestop-id-scheme/).
// The entity type prefix is "s" (stop/station). The geohash encodes the
// stop's location at precision 7 (~150m × 150m). Word breaks within the
// name component use "~" as required by the spec.
//
// If the stop's coordinates are invalid (NaN/Inf), a two-component fallback
// is used without a geohash:
//
//	"s-<dataSource>~<normalizedName>"
//
// Stops that share the same stable ID are grouped under one parent station,
// so semantically equivalent stops across the feed automatically converge to
// the same parent. Already existing parent relations are preserved.
type StableStopParentEnforcer struct {
	// DataSource is an optional short tag prepended to the name component of
	// every generated ID to scope it to a specific data provider (e.g. "de-hvv").
	// When empty the prefix is omitted.
	DataSource string
}

// unnamedStopPlaceholder is substituted for the name segment of a stable ID
// when a stop has no usable name (empty, or only punctuation/whitespace).
const unnamedStopPlaceholder = "unnamed"

// geohashPrecision is the number of geohash characters used in stable IDs.
// Precision 7 gives a ~150m × 150m bounding box — fine enough to distinguish
// nearby stops while coarse enough to be stable under minor coordinate drift.
const geohashPrecision = 7

// maxParentIDAttempts bounds the linear-probing loop in safeParentID.
const maxParentIDAttempts = 10000

var nonAlphaNumRe = regexp.MustCompile(`[^a-z0-9]+`)

// transliterateToASCII decomposes Unicode via NFD and drops combining marks,
// mapping e.g. ä→a, ö→o, ü→u. ß has no NFD form that yields "ss", so it is
// handled explicitly beforehand.
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

// normalizeStopName returns a lower-cased, tilde-separated, ASCII-only version
// of name suitable for use as the name component of a Onestop ID. Unicode
// characters are transliterated to their ASCII base letters, then any run of
// non-alphanumeric characters is collapsed to a single "~" (the Onestop ID
// word-break separator), and leading/trailing tildes are trimmed.
//
// If the result is empty, unnamedStopPlaceholder is returned.
func normalizeStopName(name string) string {
	normalized := strings.Trim(
		nonAlphaNumRe.ReplaceAllString(strings.ToLower(transliterateToASCII(name)), "~"),
		"~",
	)
	if normalized == "" {
		return unnamedStopPlaceholder
	}
	return normalized
}

// nameComponent builds the full name segment of the Onestop ID, optionally
// prefixed with dataSource separated by "~".
func nameComponent(dataSource, normalizedName string) string {
	if dataSource != "" {
		return dataSource + "~" + normalizedName
	}
	return normalizedName
}

// stableStopID returns a Onestop ID for a stop with the given attributes.
func stableStopID(dataSource, name string, lat, lon float32) string {
	normalized := normalizeStopName(name)
	namePart := nameComponent(dataSource, normalized)

	if math.IsNaN(float64(lat)) || math.IsNaN(float64(lon)) ||
		math.IsInf(float64(lat), 0) || math.IsInf(float64(lon), 0) {
		// Two-component fallback: no geohash.
		return "s-" + namePart
	}

	gh := geohash.EncodeWithPrecision(float64(lat), float64(lon), geohashPrecision)
	return "s-" + gh + "-" + namePart
}

// fallbackStableID is retained for direct use in tests that need to exercise
// the degraded path without depending on specific coordinate values.
func fallbackStableID(dataSource, normalized string, _, _ float32) string {
	return "s-" + nameComponent(dataSource, normalized)
}

// truncateToDP truncates v toward zero to dp decimal places.
// Kept for use by tests; no longer used in ID generation.
func truncateToDP(v float64, dp int) float64 {
	scale := math.Pow10(dp)
	return math.Trunc(v*scale) / scale
}

// safeParentID returns a feed-unique stop_id for stableKey.
func safeParentID(feed *gtfsparser.Feed, stableKey string) string {
	if existing, ok := feed.Stops[stableKey]; !ok || existing.Location_type == 1 {
		return stableKey
	}
	for i := 1; i <= maxParentIDAttempts; i++ {
		candidate := stableKey + "~" + strconv.Itoa(i)
		if existing, ok := feed.Stops[candidate]; !ok || existing.Location_type == 1 {
			return candidate
		}
	}
	panic(fmt.Sprintf(
		"safeParentID: exhausted %d candidate IDs for stable key %q; this indicates a pathological feed or a bug",
		maxParentIDAttempts, stableKey,
	))
}

// sortedStopIDs returns the IDs of feed.Stops in deterministic (sorted) order.
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

	// Build a reverse index once so collision re-pointing is O(children)
	// rather than O(all stops) per losing station.
	childrenOf := make(map[*gtfs.Stop][]*gtfs.Stop)
	for _, id := range sortedStopIDs(feed) {
		s := feed.Stops[id]
		if s.Parent_station != nil {
			childrenOf[s.Parent_station] = append(childrenOf[s.Parent_station], s)
		}
	}

	// Second pass: give every existing parent station a stable ID if it does
	// not already have one.
	stationSnapshot := make([]*gtfs.Stop, 0)
	for _, id := range sortedStopIDs(feed) {
		s := feed.Stops[id]
		if s.Location_type == 1 {
			stationSnapshot = append(stationSnapshot, s)
		}
	}

	for _, s := range stationSnapshot {
		if feed.Stops[s.Id] != s {
			continue
		}

		stableKey := stableStopID(e.DataSource, s.Name, s.Lat, s.Lon)

		if s.Id == stableKey {
			continue
		}

		winner, taken := feed.Stops[stableKey]
		if taken && winner != s {
			for _, child := range childrenOf[s] {
				child.Parent_station = winner
				childrenOf[winner] = append(childrenOf[winner], child)
			}
			delete(childrenOf, s)
			delete(feed.Stops, s.Id)
			continue
		}

		delete(feed.Stops, s.Id)
		s.Id = stableKey
		feed.Stops[stableKey] = s
		renamed++
	}

	fmt.Fprintf(os.Stdout, "done. (+%d new stations, %d renamed to stable IDs)\n", created, renamed)
}
