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
//	"1:<normalizedName>:<lat2dp>:<lon2dp>"
//
// When a DataSource tag is set the format is:
//
//	"1:<dataSource>:<normalizedName>:<lat2dp>:<lon2dp>"
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

// transliterateToASCII decomposes Unicode via NFD and drops combining marks,
// mapping e.g. ä→a, ö→o, ü→u. ß has no NFD form that yields "ss" so it is
// handled explicitly beforehand.
func transliterateToASCII(s string) string {
	s = strings.ReplaceAll(s, "ß", "ss")
	s = strings.ReplaceAll(s, "SS", "ss")
	result, _, _ := transform.String(
		transform.Chain(norm.NFD, transform.RemoveFunc(func(r rune) bool {
			return unicode.Is(unicode.Mn, r)
		})),
		s,
	)
	return result
}

// normalizeStopName returns a lower-cased, collapsed version of name: Unicode
// characters are first transliterated to their ASCII base letters (ä→a, ö→o,
// ü→u, ß→ss), then any run of non-alphanumeric characters is collapsed to a
// single space, and the result is trimmed.
func normalizeStopName(name string) string {
	re := regexp.MustCompile(`[^a-z0-9]+`)
	return strings.TrimSpace(re.ReplaceAllString(strings.ToLower(transliterateToASCII(name)), " "))
}

func stableStopID(dataSource, name string, lat, lon float32) string {
	normalized := normalizeStopName(name)

	cell, err := h3.LatLngToCell(
		h3.LatLng{
			Lat: float64(lat),
			Lng: float64(lon),
		},
		9,
	)

	if err != nil {
		latTrunc := math.Trunc(float64(lat)*100) / 100
		lonTrunc := math.Trunc(float64(lon)*100) / 100
		latStr := strconv.FormatFloat(latTrunc, 'f', 2, 64)
		lonStr := strconv.FormatFloat(lonTrunc, 'f', 2, 64)
		if dataSource != "" {
			return "1:" + dataSource + ":" + normalized + ":" + latStr + ":" + lonStr
		}
		return "1:" + normalized + ":" + latStr + ":" + lonStr
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

// safeParentID returns a feed-unique stop_id for stableKey.
// If stableKey is already taken by a non-station stop it appends ":1", ":2" …
func safeParentID(feed *gtfsparser.Feed, stableKey string) string {
	if existing, ok := feed.Stops[stableKey]; !ok || existing.Location_type == 1 {
		return stableKey
	}
	for i := 1; ; i++ {
		candidate := stableKey + ":" + strconv.Itoa(i)
		if existing, ok := feed.Stops[candidate]; !ok || existing.Location_type == 1 {
			return candidate
		}
	}
}

// Run executes the StableStopParentEnforcer on the given feed.
func (e StableStopParentEnforcer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Adding stable parent stations to parentless stops... ")

	// keyToParent maps stable-ID → existing-or-newly-created parent station.
	// Pre-populate with every existing station so we reuse them when their ID
	// matches the stable key we would compute for a parentless stop.
	keyToParent := make(map[string]*gtfs.Stop)
	for _, s := range feed.Stops {
		if s.Location_type == 1 {
			keyToParent[s.Id] = s
		}
	}

	created := 0
	renamed := 0

	for _, s := range feed.Stops {
		// Only act on regular stops (location_type=0) that have no parent yet.
		if s.Location_type != 0 || s.Parent_station != nil {
			continue
		}

		stableKey := stableStopID(e.DataSource, s.Name, s.Lat, s.Lon)

		parent, exists := keyToParent[stableKey]
		if !exists {
			// Derive a safe ID (may differ from stableKey if it collides with a
			// non-station stop that happens to have the same ID).
			parentID := safeParentID(feed, stableKey)

			// Bootstrap the parent from the child's fields so name, timezone,
			// wheelchair_boarding etc. are sensible defaults.
			newParent := *s
			newParent.Id = parentID
			newParent.Location_type = 1
			newParent.Parent_station = nil

			feed.Stops[newParent.Id] = &newParent
			parent = feed.Stops[newParent.Id]

			// Register under the canonical stable key so subsequent stops with
			// the same key reuse this parent even when parentID differs.
			keyToParent[stableKey] = parent
			created++
		}

		s.Parent_station = parent
	}

	// Second pass: give every existing parent station a stable ID if it does
	// not already have one. We derive the stable key from the station's own
	// name and position.
	//
	// Collision rule: if the stable key is already occupied by another station,
	// that other station wins — the current one keeps its original ID (it is
	// already stable enough to stay as-is, and its children remain attached).
	for _, s := range feed.Stops {
		if s.Location_type != 1 {
			continue
		}

		stableKey := stableStopID(e.DataSource, s.Name, s.Lat, s.Lon)

		if s.Id == stableKey {
			// Already has a stable ID — nothing to do.
			continue
		}

		winner, taken := feed.Stops[stableKey]
		if taken && winner != s {
			// Another station already owns that stable key — it wins.
			// Re-point every child of s that should now belong to winner.
			for _, child := range feed.Stops {
				if child.Parent_station == s {
					child.Parent_station = winner
				}
			}
			// Remove the unstable duplicate from the feed.
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
