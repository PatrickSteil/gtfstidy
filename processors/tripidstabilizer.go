// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"os"
	"strconv"
)

// TripIDStabilizer replaces trip IDs by hash-like IDs
type TripIDStabilizer struct {
}

// Run this TripIDStabilizer on a feed
func (minimizer TripIDStabilizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Stabilizing trip ids... ")

	flds := addFldsRenamers{newAddFldsRenamer(feed.TripsAddFlds), newAddFldsRenamer(feed.StopTimesAddFlds), newAddFldsRenamer(feed.FrequenciesAddFlds)}

	newMap := make(map[string]*gtfs.Trip)
	for _, t := range feed.Trips {
		oldId := t.Id

		newId := tripHash(t, "")

		var idCount int = 1
		for _, ok := newMap[newId]; ok; _, ok = newMap[newId] {
			fmt.Fprintf(os.Stdout, "Collision on trip id '%s'!\n", newId)
			newId = tripHash(t, "#"+strconv.Itoa(idCount)+"-")
			idCount += 1
		}

		t.Id = newId
		newMap[t.Id] = t

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Trips = newMap
	flds.apply()

	fmt.Fprintf(os.Stdout, "done.\n")
}

func tripHash(t *gtfs.Trip, prefix string) string {
	hasher := sha256.New()

	// route part
	routeName := t.Route.Short_name
	if len(routeName) == 0 {
		routeName = t.Route.Long_name
	}
	if len(routeName) == 0 {
		routeName = *t.Short_name
	}
	if len(t.Route.Desc) != 0 {
		hasher.Write([]byte(t.Route.Desc))
	}

	// agency part
	if len(t.Route.Agency.Name) != 0 {
		hasher.Write([]byte(t.Route.Agency.Name))
	}

	hasher.Write([]byte(routeName))

	for _, st := range t.StopTimes {
		stopName := st.Stop().Name

		depTime := timeToString(st.Departure_time())
		arrTime := timeToString(st.Arrival_time())

		hasher.Write([]byte(stopName))
		hasher.Write([]byte(depTime))
		hasher.Write([]byte(arrTime))
	}

	// headsign
	hasher.Write([]byte(*t.Headsign))
	hasher.Write([]byte(strconv.Itoa(int(t.Wheelchair_accessible))))
	hasher.Write([]byte(strconv.Itoa(int(t.Bikes_allowed))))

	first := t.Service.GetFirstActiveDate()
	hasher.Write([]byte(strconv.Itoa(int(first.Year()))))
	hasher.Write([]byte(strconv.Itoa(int(first.Month()))))
	hasher.Write([]byte(strconv.Itoa(int(first.Day()))))

	newId := ""

	if len(prefix) > 0 {
		newId = prefix
	}

	newId += hex.EncodeToString(hasher.Sum(nil))

	return newId
}

func timeToString(time gtfs.Time) string {
	return fmt.Sprintf("%02d:%02d:%02d", time.Hour, time.Minute, time.Second)
}
