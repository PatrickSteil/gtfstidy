// Copyright 2025 Patrick Steil
// Authors: patrick@steil.dev
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"os"
	"strconv"

	"github.com/patrickbr/gtfsparser"
)

type HierachicalStopIDs struct {
	Prefix string
	Base   int
}

func (minimizer HierachicalStopIDs) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Encoding stop_ids via parent_station ids ... ")

	countParentStations := 0

	for _, s := range feed.Stops {
		if s.Parent_station == nil {
			s.Id = minimizer.Prefix + s.Id
			countParentStations += 1
		}
	}

	numChildOfParent := make(map[string]int64, countParentStations)
	for _, s := range feed.Stops {
		if s.Parent_station != nil {
			nr := numChildOfParent[s.Parent_station.Id]
			numChildOfParent[s.Parent_station.Id] += 1

			s.Id = s.Parent_station.Id + ":" + strconv.FormatInt(nr, minimizer.Base)
		}
	}
	fmt.Fprintf(os.Stdout, "done.\n")
}
