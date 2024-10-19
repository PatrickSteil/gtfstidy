// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"github.com/patrickbr/gtfsparser"
	"testing"
)

func isTripInFeed(trip_id string, feed *gtfsparser.Feed) bool {
	_, ok := feed.Trips[trip_id]
	return ok
}

// Check how many of the given trip_ids are in the feed
func countTripsInFeed(trip_ids []string, feed *gtfsparser.Feed) int {
	count := 0
	for _, trip_id := range trip_ids {
		if isTripInFeed(trip_id, feed) {
			count++
		}
	}
	return count
}

// Check how many frequencies the given trip_ids contain
func countFrequenciesOfTripsInFeed(trip_ids []string, feed *gtfsparser.Feed) int {
	count := 0
	for _, trip_id := range trip_ids {
		if isTripInFeed(trip_id, feed) {
			if feed.Trips[trip_id].Frequencies != nil {
				count += len(*feed.Trips[trip_id].Frequencies)
			}
		}
	}
	return count
}
func TestFrequencyMinimizer(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed-frequency-min")

	if e != nil {
		t.Error(e)
		return
	}

	// Note: 10 min max Headway
	proc := FrequencyMinimizer{MinHeadway: 1, MaxHeadway: 600}
	proc.Run(feed)

	// These will be grouped into 1 frequency
	tripsTogether1 := []string{"trip_bus_A", "trip_bus_B", "trip_bus_C", "trip_bus_D", "trip_bus_E", "trip_bus_F"}
	// These will *not* be grouped into 1 frequency due to MaxHeadway
	tripsTogether2 := []string{"trip_train_A", "trip_train_B", "trip_train_C", "trip_train_D"}

	if countTripsInFeed(tripsTogether1, feed) != 1 {
		t.Error("Exactly one trip of the given trip list should be the representative of this frequency")
	}

	if countTripsInFeed(tripsTogether2, feed) != len(tripsTogether2) {
		t.Error("All trips of the given trip list should be the representative of this frequency")
	}

	if countFrequenciesOfTripsInFeed(tripsTogether1, feed) != 1 {
		t.Error("There are not enough frequencies being added for the group of trips!")
	}

	if countFrequenciesOfTripsInFeed(tripsTogether2, feed) != 0 {
		t.Error("There are should not be any frequencies being added for the group of trips!")
	}

	if !isTripInFeed("trip_bus_irregular_A", feed) {
		t.Error("The trip 'trip_bus_irregular_A' should be in the dataset!")
	}
	if !isTripInFeed("trip_bus_irregular_B", feed) {
		t.Error("The trip 'trip_bus_irregular_B' should be in the dataset!")
	}
	if !isTripInFeed("trip_bus_irregular_C", feed) {
		t.Error("The trip 'trip_bus_irregular_C' should be in the dataset!")
	}
	if !isTripInFeed("trip_bus_regular_diff_service", feed) {
		t.Error("The trip 'trip_bus_regular_diff_service' should be in the dataset!")
	}

	if 9 != len(feed.Trips) {
		t.Error("There are too many trips!")
	}
}
