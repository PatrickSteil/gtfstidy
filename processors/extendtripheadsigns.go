// Copyright 2025 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"os"

	"github.com/patrickbr/gtfsparser"
)

type ExtendTripHeadSign struct {
}

func (f ExtendTripHeadSign) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Extending trip headsign ... ")

	for _, t := range feed.Trips {
		// If already present, do not overwrite
		if *t.Headsign != "" {
			continue
		}

		// Take last stations name
		t.Headsign = &t.StopTimes[len(t.StopTimes)-1].Stop().Name
	}
	fmt.Fprintf(os.Stdout, "done.\n")
}
