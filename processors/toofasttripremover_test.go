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

func TestTooFastTripRemover(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed-fast")

	if e != nil {
		t.Error(e)
		return
	}

	proc := TooFastTripRemover{}
	proc.Run(feed)

	if _, ok := feed.Trips["too_fast_trip_bus"]; ok {
		t.Error("'too_fast_trip_bus' is a bus trip which is too fast!")
	}
	if _, ok := feed.Trips["too_fast_trip_train"]; ok {
		t.Error("'too_fast_trip_train' is a train trip which is too fast!")
	}
	if _, ok := feed.Trips["too_fast_trip_subway"]; ok {
		t.Error("'too_fast_trip_subway' is a subway trip which is too fast!")
	}
	if _, ok := feed.Trips["too_fast_trip_tram"]; ok {
		t.Error("'too_fast_trip_tram' is a tram trip which is too fast!")
	}
	if _, ok := feed.Trips["too_fast_trip_ferry"]; ok {
		t.Error("'too_fast_trip_ferry' is a ferry trip which is too fast!")
	}
	if _, ok := feed.Trips["too_fast_trip_subway_wrong_coordinate"]; ok {
		t.Error("'too_fast_trip_subway_wrong_coordinate' is a subway trip which is too fast!")
	}
	if _, ok := feed.Trips["too_fast_trip_subway_with_frequencies"]; ok {
		t.Error("'too_fast_trip_subway_with_frequencies' is a subway trip which is too fast!")
	}

	if 6 != len(feed.Trips) {
		t.Error("The result does not contain the correct amount of trips!")
	}

	for _, T := range feed.Trips {
		if T.Frequencies != nil && 0 != len(*T.Frequencies) {
			t.Error("The result does contain some frequencies, but those should have been removed!")
		}
	}
}
