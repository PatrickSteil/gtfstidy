// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// The ids are derived from the trips, so they don't change if the stabilizer is
// run again, and neither must the additional fields of the trips.
func TestTripIDStabilizerKeepsAdditionalFields(t *testing.T) {
	feed := gtfsparser.NewFeed()

	agency := &gtfs.Agency{Id: "a", Name: "Agency"}
	route := &gtfs.Route{Id: "r", Agency: agency, Short_name: "R1"}
	stop := &gtfs.Stop{Id: "s", Name: "Stop"}

	service := gtfs.EmptyService()
	service.SetId("service")
	service.SetExceptionTypeOn(gtfs.NewDate(5, 1, 2026), 1)

	headsign := "Hbf"
	trip := &gtfs.Trip{Id: "original", Route: route, Service: service, Headsign: &headsign}

	var st gtfs.StopTime
	st.SetStop(stop)
	st.SetSequence(1)
	trip.StopTimes = append(trip.StopTimes, st)

	freq := &gtfs.Frequency{Headway_secs: 600}

	feed.Trips["original"] = trip
	feed.TripsAddFlds["extra"] = map[string]string{"original": "trip value"}
	feed.StopTimesAddFlds["st_extra"] = map[string]map[int]string{"original": {1: "stop time value"}}
	feed.FrequenciesAddFlds["freq_extra"] = map[string]map[*gtfs.Frequency]string{"original": {freq: "frequency value"}}

	TripIDStabilizer{}.Run(feed)

	stableId := trip.Id
	if stableId == "original" {
		t.Fatal("expected the trip id to change")
	}

	TripIDStabilizer{}.Run(feed)

	if trip.Id != stableId {
		t.Fatalf("expected the id of the trip to stay %q, got %q", stableId, trip.Id)
	}

	if got := feed.TripsAddFlds["extra"][trip.Id]; got != "trip value" {
		t.Errorf("additional trip field is %q, expected %q", got, "trip value")
	}

	if got := feed.StopTimesAddFlds["st_extra"][trip.Id][1]; got != "stop time value" {
		t.Errorf("additional stop time field is %q, expected %q", got, "stop time value")
	}

	if got := feed.FrequenciesAddFlds["freq_extra"][trip.Id][freq]; got != "frequency value" {
		t.Errorf("additional frequency field is %q, expected %q", got, "frequency value")
	}
}
