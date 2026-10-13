// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
	"os"
)

// NonEssentialDropper removes everything from a feed that is not needed
// for plain timetable data, leaving agencies, stops, routes, trips, stop
// times, calendars, frequencies and transfers. Shapes are dropped at parse
// time (see ParseOptions.DropShapes).
type NonEssentialDropper struct {
}

// Run this NonEssentialDropper on some feed
func (d NonEssentialDropper) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Dropping non-essential data (fares, levels, pathways, attributions, translations, feed info)... ")

	numFares := len(feed.FareAttributes)
	numLevels := len(feed.Levels)
	numPathways := len(feed.Pathways)
	numAttrs := len(feed.Attributions)
	numFeedInfos := len(feed.FeedInfos)

	feed.FareAttributes = make(map[string]*gtfs.FareAttribute)
	feed.FareAttributesAddFlds = make(map[string]map[string]string)
	feed.FareRulesAddFlds = make(map[string]map[string]map[*gtfs.FareAttributeRule]string)
	feed.ZoneIds = make(map[string]bool)

	feed.Levels = make(map[string]*gtfs.Level)
	feed.LevelsAddFlds = make(map[string]map[string]string)

	feed.Pathways = make(map[string]*gtfs.Pathway)
	feed.PathwaysAddFlds = make(map[string]map[string]string)

	feed.Attributions = nil
	feed.AttributionsAddFlds = make(map[string]map[*gtfs.Attribution]string)

	feed.FeedInfos = nil
	feed.FeedInfosAddFlds = make(map[string]map[*gtfs.FeedInfo]string)

	feed.TranslationsAddFlds = make(map[string]map[*gtfs.Translation]string)

	for _, s := range feed.Stops {
		s.Level = nil
		s.Translations = nil
	}

	for _, a := range feed.Agencies {
		a.Attributions = nil
		a.Translations = nil
		a.Fare_url = nil
	}

	for _, r := range feed.Routes {
		r.Attributions = nil
	}

	for _, t := range feed.Trips {
		t.Attributions = nil
		t.Translations = nil
	}

	fmt.Fprintf(os.Stdout, "done. (-%d fares, -%d levels, -%d pathways, -%d attributions, -%d feed infos)\n", numFares, numLevels, numPathways, numAttrs, numFeedInfos)
}
