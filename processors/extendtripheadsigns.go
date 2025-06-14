// Copyright 2025 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/patrickbr/gtfsparser"
)

type ExtendTripHeadSign struct {
}

func (f ExtendTripHeadSign) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Extending trip headsign ... ")

	maxThreads := runtime.NumCPU()
	sem := make(chan struct{}, maxThreads)

	var wg sync.WaitGroup

	for _, t := range feed.Trips {
		if *t.Headsign != "" {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)

		trip := t
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			trip.Headsign = &trip.StopTimes[len(trip.StopTimes)-1].Stop().Name
		}()
	}

	wg.Wait()
	fmt.Fprintf(os.Stdout, "done.\n")
}
