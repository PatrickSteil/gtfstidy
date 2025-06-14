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
	// Remove diacritics (e.g. "ü" -> "u", "é" -> "e")
	name = unidecode.Unidecode(name)

	// Convert to lowercase for case-insensitive comparison
	name = strings.ToLower(name)

	// Remove any parenthetical content (e.g. "Hbf (tief)" -> "Hbf")
	name = regexp.MustCompile(`\s*\([^)]*\)`).ReplaceAllString(name, "")

	// Replace hyphens with spaces (e.g. "Frankfurt-Süd" -> "Frankfurt Süd")
	name = strings.ReplaceAll(name, "-", " ")

	// Remove punctuation (excluding letters, numbers, and whitespace)
	name = regexp.MustCompile(`[^\w\s]`).ReplaceAllString(name, "")

	// Remove platform/track/vehicle references (e.g. "Bussteig", "Gleis")
	name = regexp.MustCompile(`\b(bussteig|gleis|bahnsteig|bus|zug)\b`).ReplaceAllString(name, "")

	// Remove transit agency/operator names (e.g. "DB", "MVV")
	name = regexp.MustCompile(`\b(db|mvv|vrr|rmv|bvg|sbb|oebb|sncf|trenitalia)\b`).ReplaceAllString(name, "")

	// Normalize whitespace (collapse multiple spaces into one)
	name = strings.Join(strings.Fields(name), " ")

	// Replace common long terms with abbreviations
	replacements := map[string]string{
		// German terms
		" hauptbahnhof": " hbf",
		" bahnhof":      " bf",
		"hauptbahnhof ": "hbf ",
		"bahnhof ":      "bf ",
		"strasse":       "str",
		"platz":         "pl",

		// English terms
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

	// Remove remaining transport-related abbreviations or prefixes
	// (e.g. "S", "U", "RB", "RE", "Tram", "Bus", "Bhf")
	re := regexp.MustCompile(`\b(s\+u|s|u|rb|re|tram|bus|bhf)\b[ \.]?`)
	name = re.ReplaceAllString(name, "")

	// Final whitespace cleanup
	name = strings.Join(strings.Fields(name), " ")

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

	root := BuildKDTreeParallelLimited(points, 0)

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

	// var mu sync.Mutex
	// var wg sync.WaitGroup

	// // Parallelize uf.Apply calls
	// uf.Apply(func(key, parent string) {
	// 	wg.Add(1)
	// 	go func(key, parent string) {
	// 		defer wg.Done()

	// 		parID := "par::" + parent

	// 		mu.Lock()
	// 		_, ok := feed.Stops[parID]
	// 		mu.Unlock()

	// 		if !ok {
	// 			mu.Lock()
	// 			// Double check inside lock to avoid race
	// 			if _, stillNotExist := feed.Stops[parID]; stillNotExist == false {
	// 				orig := feed.Stops[parent]
	// 				feed.Stops[parID] = createParentStopFrom(orig, parID)
	// 			}
	// 			mu.Unlock()
	// 		}

	// 		mu.Lock()
	// 		stop := feed.Stops[key]
	// 		stop.Parent_station = feed.Stops[parID]
	// 		mu.Unlock()
	// 	}(key, parent)
	// })

	// wg.Wait()

	// // Parallelize the second loop with mutex on map writes
	// for _, stop := range feed.Stops {
	// 	if stop.Location_type == 0 && stop.Parent_station == nil {
	// 		wg.Add(1)
	// 		go func(stop *gtfs.Stop) {
	// 			defer wg.Done()

	// 			parID := "par::" + stop.Id

	// 			mu.Lock()
	// 			_, exists := feed.Stops[parID]
	// 			mu.Unlock()

	// 			if !exists {
	// 				mu.Lock()
	// 				if _, stillNotExist := feed.Stops[parID]; stillNotExist == false {
	// 					feed.Stops[parID] = createParentStopFrom(stop, parID)
	// 				}
	// 				mu.Unlock()
	// 			}

	// 			mu.Lock()
	// 			stop.Parent_station = feed.Stops[parID]
	// 			mu.Unlock()
	// 		}(stop)
	// 	}
	// }

	// wg.Wait()

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
