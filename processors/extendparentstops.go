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

var TOL_IS_SAME = 85

const radiusKm = 0.5

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
		// German terms you have:
		" hauptbahnhof": " hbf",
		" bahnhof":      " bf",
		"hauptbahnhof ": "hbf ",
		"bahnhof ":      "bf ",
		"strasse":       "str",
		"platz":         "pl",

		// English terms:
		" station":      " stn",
		" street":       " st",
		" avenue":       " ave",
		" boulevard":    " blvd",
		" road":         " rd",
		" drive":        " dr",
		" court":        " ct",
		" square":       " sq",
		" parkway":      " pkwy",
		" highway":      " hwy",
		" circle":       " cir",
		" lane":         " ln",
		" place":        " pl",
		" terrace":      " ter",
		" expressway":   " expy",
		" junction":     " jct",
		" intersection": " int",
		" terminal":     " term",
		" airport":      " apt",
		" downtown":     " dtwn",
		" ferry":        " fry",
	}

	for k, v := range replacements {
		name = strings.ReplaceAll(name, k, v)
	}

	// Remove common public transport prefixes or suffixes (e.g. s, u, rb, tram, bus, bhf)
	re := regexp.MustCompile(`\b(s\+u|s|u|rb|re|tram|bus|bhf)\b[ \.]?`)
	name = re.ReplaceAllString(name, "")

	return name
}

func createParentStopFrom(orig *gtfs.Stop, id string) *gtfs.Stop {
	return &gtfs.Stop{
		Id:            id,
		Name:          orig.Name,
		Lat:           orig.Lat,
		Lon:           orig.Lon,
		Location_type: 1,
		Timezone:      orig.Timezone,
	}
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

	points := make([]Point[*gtfs.Stop], 0, len(feed.Stops))
	for _, s := range feed.Stops {
		points = append(points, Point[*gtfs.Stop]{Lat: float64(s.Lat), Lon: float64(s.Lon), Data: s})
		uf.InitKey(s.Id)
	}

	root := BuildKDTree(points, 0)

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
		for _, p := range results {
			o := p.Data
			if ConsiderSame(norm, normalize(o.Name), TOL_IS_SAME) {
				uf.UnionSet(s.Id, o.Id)
			}
		}
	}

	// First, ensure all existing parent_station references are remapped to "par::<id>" versions
	for _, stop := range feed.Stops {
		if stop.Parent_station != nil {
			parent := stop.Parent_station
			parID := "par::" + parent.Id
			if _, exists := feed.Stops[parID]; !exists {
				feed.Stops[parID] = createParentStopFrom(stop, parID)
			}
			stop.Parent_station = feed.Stops[parID]
		}
	}

	// Apply the union-find hierarchy to assign canonical parent stations
	uf.Apply(func(key, parent string) {
		stop := feed.Stops[key]
		parID := "par::" + parent

		if _, ok := feed.Stops[parID]; !ok {
			orig := feed.Stops[parent]
			feed.Stops[parID] = createParentStopFrom(orig, parID)
		}
		stop.Parent_station = feed.Stops[parID]
	})

	// Ensure all stops have a parent station
	for _, stop := range feed.Stops {
		if stop.Location_type == 0 && stop.Parent_station == nil {
			parID := "par::" + stop.Id
			if _, exists := feed.Stops[parID]; !exists {
				feed.Stops[parID] = createParentStopFrom(stop, parID)
			}
			stop.Parent_station = feed.Stops[parID]
		}
	}

	after := uf.NumDisjointSets()

	if after < prev {
		fmt.Fprintf(os.Stdout, "(+%d parent stops [+%.2f%%]) done.\n", (prev - after), 100.0*float64(prev-after)/float64(prev))
	} else {
		fmt.Fprintf(os.Stdout, "(-%d parent stops [-%.2f%%]) done.\n", (after - prev), 100.0*float64(after-prev)/float64(prev))
	}
}
