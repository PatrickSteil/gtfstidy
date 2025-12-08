package processors

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

func applyMultilingualReplacements(name string) string {
	n := strings.ToLower(name)

	replacements := []struct {
		from string
		to   string
	}{
		// --- German ---
		{"straße", "strasse"},
		{"str.", "strasse"},
		{" str ", " strasse "},
		{"str ", "strasse "},
		{"ä", "ae"},
		{"ö", "oe"},
		{"ü", "ue"},
		{"ß", "ss"},
		{" hbf", " hauptbahnhof"},
		{" hbf.", " hauptbahnhof"},
		{"bf.", "bahnhof"},

		// --- English ---
		{"station", "sta"},
		{"railway", "rail"},
		{"railroad", "rail"},
		{"train station", "sta"},

		// --- Dutch ---
		{"straat", "str"},
		{"plein", "pl"},
		{"cs", "centraal"},

		// --- French ---
		{"gare", "ga"},
		{"st ", "saint "},
		{"é", "e"},
		{"è", "e"},
		{"ê", "e"},
		{"ë", "e"},
		{"à", "a"},
		{"â", "a"},
		{"î", "i"},
		{"ô", "o"},
		{"û", "u"},
		{"ç", "c"},
	}

	for _, r := range replacements {
		n = strings.ReplaceAll(n, r.from, r.to)
	}

	n = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == ' ' {
			return r
		}
		return -1
	}, n)

	// --- Collapse multiple spaces ---
	n = strings.Join(strings.Fields(n), " ")

	// --- Replace spaces with "_" ---
	n = strings.ReplaceAll(n, " ", "_")

	return n
}

type StableStopIdProcessors struct {
	Precision int
}

func (m StableStopIdProcessors) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Computing stable stop ids ... ")

	newMap := make(map[string]*gtfs.Stop)
	for _, s := range feed.Stops {
		// skip stops without lat lon and process only parent stations
		if !s.HasLatLon() || s.Parent_station != nil {
			newMap[s.Id] = s
			continue
		}

		oldId := s.Id

		geoHash := geohash(float64(s.Lat), float64(s.Lon), m.Precision)
		normalizedName := applyMultilingualReplacements(s.Name)

		newId := "s::" + geoHash + ":" + normalizedName
		s.Id = newId

		newMap[s.Id] = s

		// update additional fields
		for k := range feed.StopsAddFlds {
			feed.StopsAddFlds[k][newId] = feed.StopsAddFlds[k][oldId]
			delete(feed.StopsAddFlds[k], oldId)
		}
	}
	feed.Stops = newMap
	fmt.Fprintf(os.Stdout, "done.\n")
}
