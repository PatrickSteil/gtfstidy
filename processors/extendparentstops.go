// Copyright 2025 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/mozillazg/go-unidecode"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	fuzzy "github.com/paul-mannino/go-fuzzywuzzy"
)

// https://github.com/dhconnelly/rtreego
var REC_TOL = 0.01
var TOL_IS_SAME = 85

const radiusKm = 1.0

func normalize(name string) string {
	// Remove diacritics (e.g. ü -> u)
	name = unidecode.Unidecode(name)

	// Convert all letters to lowercase
	name = strings.ToLower(name)

	// Add a space between any character and '(' if missing
	// e.g. "Hbf(Frankfurt)" -> "Hbf (Frankfurt)"
	name = regexp.MustCompile(`(\S)\(`).ReplaceAllString(name, "$1 (")

	// Add a space between ')' and any character if missing
	// e.g. "Hbf (Frankfurt)Main" -> "Hbf (Frankfurt) Main"
	name = regexp.MustCompile(`\)(\S)`).ReplaceAllString(name, ") $1")

	// Replace hyphens with a space
	name = strings.ReplaceAll(name, "-", " ")

	// Remove punctuation characters except whitespace and word characters
	name = regexp.MustCompile(`[^\w\s]`).ReplaceAllString(name, "")

	// Collapse multiple whitespace characters into a single space
	name = strings.Join(strings.Fields(name), " ")

	// Replace common long terms with their abbreviations
	replacements := map[string]string{
		" hauptbahnhof": " hbf",
		" bahnhof":      " bf",
		"hauptbahnhof ": "hbf ",
		"bahnhof ":      "bf ",
		"strasse":       "str",
		"platz":         "pl",
	}

	for k, v := range replacements {
		name = strings.ReplaceAll(name, k, v)
	}

	// Remove common public transport prefixes or suffixes (e.g. s, u, rb, tram, bus, bhf)
	re := regexp.MustCompile(`\b(s\+u|s|u|rb|re|tram|bus|bhf)\b[ \.]?`)
	name = re.ReplaceAllString(name, "")

	return name
}

// should two names be considered "the same"
func ConsiderSame(left, right string, threshold int) bool {
	similarity := fuzzy.Ratio(normalize(left), normalize(right))
	return bool(similarity >= threshold)
}

type ExtendParentStops struct {
	DiscardByRouteType bool
}

func (f ExtendParentStops) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Extending parent stops ... ")

	uf := NewUnionFind[string]()

	// Build KD-tree of stops
	var root *Node[*gtfs.Stop]
	for _, s := range feed.Stops {
		root = Insert(root, Point[*gtfs.Stop]{Lat: float64(s.Lat), Lon: float64(s.Lon), Data: s}, 0)
		uf.InitKey(s.Id)

		if s.Parent_station != nil {
			uf.MarkAsParent(s.Parent_station.Id)
		}
	}

	for _, s := range feed.Stops {
		if s.Parent_station != nil {
			uf.UnionSet(s.Id, s.Parent_station.Id)
		}
	}

	prev := uf.NumDisjointSets()

	for _, s := range feed.Stops {
		query := Point[*gtfs.Stop]{Lat: float64(s.Lat), Lon: float64(s.Lon), Data: s}
		var results []Point[*gtfs.Stop]
		SearchRange(root, query, radiusKm, 0, &results)

		norm := normalize(s.Name)

		// fmt.Printf("From %v (%v)\n", s.Name, norm)

		for _, p := range results {
			o := p.Data
			// fmt.Printf("> To %v (%v)\n", o.Name, normalize(o.Name))

			if ConsiderSame(norm, normalize(o.Name), TOL_IS_SAME) {
				uf.UnionSet(s.Id, o.Id)
			}
		}
	}

	uf.Apply(func(key, parent string) {
		if key != parent {
			feed.Stops[key].Parent_station = feed.Stops[parent]
		}
	})

	after := uf.NumDisjointSets()

	if after < prev {
		fmt.Fprintf(os.Stdout, "(+%d parent stops [+%.2f%%]) done.\n", (prev - after), 100.0*float64(prev-after)/float64(prev))
	} else {
		fmt.Fprintf(os.Stdout, "(-%d parent stops [-%.2f%%]) done.\n", (after - prev), 100.0*float64(after-prev)/float64(prev))
	}
}
