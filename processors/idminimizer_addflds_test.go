// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

func TestAddFldsRenamer(t *testing.T) {
	flds := map[string]map[string]string{
		"a": {"1": "a1", "2": "a2", "stale": "stale value"},
		"b": {"2": "b2"},
		"c": {},
	}

	r := newAddFldsRenamer(flds)

	// swap two ids, and rename an id that has no values
	r.rename("1", "2")
	r.rename("2", "1")
	r.rename("3", "3")

	// nothing happens before apply
	if flds["a"]["1"] != "a1" {
		t.Fatalf("expected the values to stay in place until apply, got %v", flds)
	}

	r.apply()

	want := map[string]map[string]string{
		"a": {"2": "a1", "1": "a2"},
		"b": {"1": "b2"},
		"c": {},
	}

	if !reflect.DeepEqual(flds, want) {
		t.Errorf("expected %v, got %v", want, flds)
	}
}

// The new ids are 1, 2, 3, ..., which are also the old ids in this test, so the
// additional field values have to follow the entities to their new id.
func TestIDMinimizerKeepsStopAdditionalFields(t *testing.T) {
	// the order in which the new ids are assigned is random
	for run := 0; run < 20; run++ {
		feed := gtfsparser.NewFeed()
		feed.StopsAddFlds["extra"] = make(map[string]string)

		want := make(map[*gtfs.Stop]string)

		for i := 1; i <= 5; i++ {
			id := strconv.Itoa(i)
			s := &gtfs.Stop{Id: id}

			feed.Stops[id] = s
			feed.StopsAddFlds["extra"][id] = "value of " + id
			want[s] = "value of " + id
		}

		IDMinimizer{Base: 10}.Run(feed)

		for s, value := range want {
			if got := feed.StopsAddFlds["extra"][s.Id]; got != value {
				t.Fatalf("run %d: stop %q has additional field %q, expected %q", run, s.Id, got, value)
			}
		}

		if len(feed.StopsAddFlds["extra"]) != len(want) {
			t.Fatalf("run %d: expected %d additional field values, got %v", run, len(want), feed.StopsAddFlds["extra"])
		}
	}
}

func TestIDMinimizerKeepsTripAdditionalFields(t *testing.T) {
	for run := 0; run < 20; run++ {
		feed := gtfsparser.NewFeed()
		feed.TripsAddFlds["trip_extra"] = make(map[string]string)
		feed.StopTimesAddFlds["st_extra"] = make(map[string]map[int]string)
		feed.FrequenciesAddFlds["freq_extra"] = make(map[string]map[*gtfs.Frequency]string)

		type values struct {
			trip, stopTime, frequency string
		}
		want := make(map[*gtfs.Trip]values)
		freqs := make(map[*gtfs.Trip]*gtfs.Frequency)

		for i := 1; i <= 4; i++ {
			id := strconv.Itoa(i)
			trip := &gtfs.Trip{Id: id}
			freq := &gtfs.Frequency{Headway_secs: 600}

			var st gtfs.StopTime
			st.SetSequence(7)
			trip.StopTimes = append(trip.StopTimes, st)

			feed.Trips[id] = trip
			feed.TripsAddFlds["trip_extra"][id] = "trip " + id
			feed.StopTimesAddFlds["st_extra"][id] = map[int]string{7: "stop time " + id}
			feed.FrequenciesAddFlds["freq_extra"][id] = map[*gtfs.Frequency]string{freq: "frequency " + id}

			want[trip] = values{"trip " + id, "stop time " + id, "frequency " + id}
			freqs[trip] = freq
		}

		IDMinimizer{Base: 10}.Run(feed)

		for trip, w := range want {
			if got := feed.TripsAddFlds["trip_extra"][trip.Id]; got != w.trip {
				t.Fatalf("run %d: trip %q has additional field %q, expected %q", run, trip.Id, got, w.trip)
			}

			if got := feed.StopTimesAddFlds["st_extra"][trip.Id][7]; got != w.stopTime {
				t.Fatalf("run %d: trip %q has additional stop time field %q, expected %q", run, trip.Id, got, w.stopTime)
			}

			if got := feed.FrequenciesAddFlds["freq_extra"][trip.Id][freqs[trip]]; got != w.frequency {
				t.Fatalf("run %d: trip %q has additional frequency field %q, expected %q", run, trip.Id, got, w.frequency)
			}
		}
	}
}

// All other entities with id 1 also get id 1 again, and the fares have to keep the
// additional fields of their rules.
func TestIDMinimizerKeepsAdditionalFieldsOfOtherEntities(t *testing.T) {
	feed := gtfsparser.NewFeed()

	feed.Agencies["1"] = &gtfs.Agency{Id: "1"}
	feed.AgenciesAddFlds["extra"] = map[string]string{"1": "agency"}

	feed.Routes["1"] = &gtfs.Route{Id: "1"}
	feed.RoutesAddFlds["extra"] = map[string]string{"1": "route"}

	feed.Shapes["1"] = &gtfs.Shape{Id: "1"}
	feed.ShapesAddFlds["extra"] = map[string]map[int]string{"1": {3: "shape point"}}

	feed.Levels["1"] = &gtfs.Level{Id: "1"}
	feed.LevelsAddFlds["extra"] = map[string]string{"1": "level"}

	feed.Pathways["1"] = &gtfs.Pathway{Id: "1"}
	feed.PathwaysAddFlds["extra"] = map[string]string{"1": "pathway"}

	// the fares get the ids 1 and 2, whichever of them is which
	rule1 := &gtfs.FareAttributeRule{}
	fare1 := &gtfs.FareAttribute{Id: "1", Rules: []*gtfs.FareAttributeRule{rule1}}
	rule2 := &gtfs.FareAttributeRule{}
	fare2 := &gtfs.FareAttribute{Id: "fare-b", Rules: []*gtfs.FareAttributeRule{rule2}}

	feed.FareAttributes["1"] = fare1
	feed.FareAttributes["fare-b"] = fare2
	feed.FareAttributesAddFlds["extra"] = map[string]string{"1": "fare 1", "fare-b": "fare b"}
	feed.FareRulesAddFlds["extra"] = map[string]map[*gtfs.FareAttributeRule]string{"1": {rule1: "rule 1"}, "fare-b": {rule2: "rule b"}}

	IDMinimizer{Base: 10}.Run(feed)

	checks := []struct {
		name      string
		got, want string
	}{
		{"agency", feed.AgenciesAddFlds["extra"]["1"], "agency"},
		{"route", feed.RoutesAddFlds["extra"]["1"], "route"},
		{"shape point", feed.ShapesAddFlds["extra"]["1"][3], "shape point"},
		{"level", feed.LevelsAddFlds["extra"]["1"], "level"},
		{"pathway", feed.PathwaysAddFlds["extra"]["1"], "pathway"},
		{"fare 1", feed.FareAttributesAddFlds["extra"][fare1.Id], "fare 1"},
		{"fare b", feed.FareAttributesAddFlds["extra"][fare2.Id], "fare b"},
		{"fare rule 1", feed.FareRulesAddFlds["extra"][fare1.Id][rule1], "rule 1"},
		{"fare rule b", feed.FareRulesAddFlds["extra"][fare2.Id][rule2], "rule b"},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("additional field of %s is %q, expected %q", c.name, c.got, c.want)
		}
	}
}
