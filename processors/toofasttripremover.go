// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"os"
)

// Define distance thresholds for different route types in kilometers
var distanceThresholds = map[int16]float64{
	0:  1,  // Type 0: Tram, Streetcar, Light rail
	1:  1,  // Type 1: Subway, Metro
	2:  10, // Type 2: Rail
	3:  1,  // Type 3: Bus
	4:  5,  // Type 4: Ferry
	5:  5,  // Type 5: Cable Car
	6:  5,  // Type 6: Aerial lift
	7:  5,  // Type 7: Funicular
	11: 5,  // Type 11: Trolleybus
	12: 5,  // Type 12: Monorail
}

// StopDuplicateRemover merges semantically equivalent stops
type TooFastTripRemover struct {
}

// Run this StopDuplicateRemover on some feed
func (f TooFastTripRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing trips travelling too fast...")

	bef := len(feed.Trips)

	for id, t := range feed.Trips {
		if len(t.StopTimes) == 0 {
			continue
		}

		last := t.StopTimes[0]
		dist := 0.0

		for i := 1; i < len(t.StopTimes); i++ {
			dist += distSApprox(t.StopTimes[i-1].Stop(), t.StopTimes[i].Stop())

			inter := t.StopTimes[i].Arrival_time().SecondsSinceMidnight() - last.Departure_time().SecondsSinceMidnight()

			speed := 0.0 // Speed in km/h

			if inter == 0 {
				speed = (float64(dist) / 1000.0) / (float64(60) / 3600.0)
			} else {
				speed = (float64(dist) / 1000.0) / (float64(inter) / 3600.0)
			}

			routeType := gtfs.GetTypeFromExtended(t.Route.Type)
			if dist >= 1000*distanceThresholds[routeType] {
				// Route type speed limits (in km/h):
				// 0: Tram/light rail (100 km/h)
				// 1: Subway (150 km/h)
				// 2: Rail (500 km/h)
				// 3: Bus (150 km/h)
				// 4: Ferry (80 km/h)
				// 5: Cable car (30 km/h)
				// 6: Gondola (50 km/h)
				// 7: Funicular (50 km/h)
				// 11: Trolleybus (50 km/h)
				// 12: Monorail (150 km/h)

				if (routeType == 0 && speed > 100) || // Tram
					(routeType == 1 && speed > 150) || // Subway
					(routeType == 2 && speed > 500) || // Rail
					(routeType == 3 && speed > 150) || // Bus
					(routeType == 4 && speed > 80) || // Ferry
					(routeType == 5 && speed > 30) || // Cable car
					(routeType == 6 && speed > 50) || // Gondola
					(routeType == 7 && speed > 50) || // Funicular
					(routeType == 11 && speed > 50) || // Trolleybus
					(routeType == 12 && speed > 150) { // Monorail
					// Delete the trip if it exceeds the speed limit for its route type
					feed.DeleteTrip(id)
					break
				}
			}

			if inter != 0 {
				last = t.StopTimes[i]
				dist = 0
			}
		}
	}

	for id, t := range feed.Trips {
		if len(t.StopTimes) == 0 {
			continue
		}

		for j := 1; j < len(t.StopTimes); j++ {
			dist := 0.0
			for i := j + 1; i < len(t.StopTimes); i++ {
				dist += distSApprox(t.StopTimes[i-1].Stop(), t.StopTimes[i].Stop())

				inter := t.StopTimes[i].Arrival_time().SecondsSinceMidnight() - t.StopTimes[j].Departure_time().SecondsSinceMidnight()

				speed := 0.0

				if inter == 0 {
					speed = (float64(dist) / 1000.0) / (float64(60) / 3600.0)
				} else {
					speed = (float64(dist) / 1000.0) / (float64(inter) / 3600.0)
				}

				routeType := gtfs.GetTypeFromExtended(t.Route.Type)
				if dist >= 1000*distanceThresholds[routeType] {
					if (routeType == 0 && speed > 100) || // Tram
						(routeType == 1 && speed > 150) || // Subway
						(routeType == 2 && speed > 500) || // Rail
						(routeType == 3 && speed > 150) || // Bus
						(routeType == 4 && speed > 80) || // Ferry
						(routeType == 5 && speed > 30) || // Cable car
						(routeType == 6 && speed > 50) || // Gondola
						(routeType == 7 && speed > 50) || // Funicular
						(routeType == 11 && speed > 50) || // Trolleybus
						(routeType == 12 && speed > 150) { // Monorail
						// Delete the trip if it exceeds the speed limit for its route type
						feed.DeleteTrip(id)
						break
					}
				}
			}
		}
	}

	// delete transfers
	feed.CleanTransfers()

	fmt.Fprintf(os.Stdout, "done. (-%d trips [-%.2f%%])\n",
		bef-len(feed.Trips),
		100.0*float64(bef-len(feed.Trips))/(float64(bef)+0.001))
}
