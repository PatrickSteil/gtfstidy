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
	"strconv"
)

// IDMinimizer minimizes IDs by replacing them be continuous integer
type IDMinimizer struct {
	Prefix           string
	Base             int
	KeepStations     bool
	KeepBlocks       bool
	KeepTrips        bool
	KeepRoutes       bool
	KeepFares        bool
	KeepShapes       bool
	KeepLevels       bool
	KeepServices     bool
	KeepAgencies     bool
	KeepPathways     bool
	KeepAttributions bool
}

// An addFldsRenamer moves the additional field values (field name -> id -> value) of
// entities to their new id. The values are collected under the new ids while the old
// ones are still in place, and replace them in apply(). Moving them one by one would
// overwrite (or delete) values if a new id is the old id of another entity.
type addFldsRenamer[V any] struct {
	flds    map[string]map[string]V
	renamed map[string]map[string]V
}

func newAddFldsRenamer[V any](flds map[string]map[string]V) *addFldsRenamer[V] {
	return &addFldsRenamer[V]{flds, make(map[string]map[string]V, len(flds))}
}

func (r *addFldsRenamer[V]) rename(oldId string, newId string) {
	for k, vals := range r.flds {
		if v, ok := vals[oldId]; ok {
			if r.renamed[k] == nil {
				r.renamed[k] = make(map[string]V)
			}
			r.renamed[k][newId] = v
		}
	}
}

func (r *addFldsRenamer[V]) apply() {
	for k := range r.flds {
		if r.renamed[k] == nil {
			r.flds[k] = make(map[string]V)
		} else {
			r.flds[k] = r.renamed[k]
		}
	}
}

// addFldsRenamers renames the additional fields of different types at once
type addFldsRenamers []interface {
	rename(oldId string, newId string)
	apply()
}

func (rs addFldsRenamers) rename(oldId string, newId string) {
	for _, r := range rs {
		r.rename(oldId, newId)
	}
}

func (rs addFldsRenamers) apply() {
	for _, r := range rs {
		r.apply()
	}
}

// Run this IDMinimizer on a feed
func (minimizer IDMinimizer) Run(feed *gtfsparser.Feed) {
	j := 10
	if minimizer.KeepStations {
		j = j - 1
	}
	if minimizer.KeepTrips {
		j = j - 1
	}
	if minimizer.KeepRoutes {
		j = j - 1
	}
	if minimizer.KeepShapes {
		j = j - 1
	}
	if minimizer.KeepAgencies {
		j = j - 1
	}
	if minimizer.KeepServices {
		j = j - 1
	}
	if minimizer.KeepFares {
		j = j - 1
	}
	if minimizer.KeepLevels {
		j = j - 1
	}
	if minimizer.KeepPathways {
		j = j - 1
	}
	if minimizer.KeepAttributions {
		j = j - 1
	}
	fmt.Fprintf(os.Stdout, "Minimizing ids... ")
	sem := make(chan empty, j)

	if !minimizer.KeepTrips {
		go func() {
			minimizer.minimizeTripIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepStations {
		go func() {
			minimizer.minimizeStopIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepRoutes {
		go func() {
			minimizer.minimizeRouteIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepShapes {
		go func() {
			minimizer.minimizeShapeIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepAgencies {
		go func() {
			minimizer.minimizeAgencyIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepServices {
		go func() {
			minimizer.minimizeServiceIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepFares {
		go func() {
			minimizer.minimizeFareIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepPathways {
		go func() {
			minimizer.minimizePathwayIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepLevels {
		go func() {
			minimizer.minimizeLevelIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepAttributions {
		go func() {
			minimizer.minimizeAttributionIds(feed)
			sem <- empty{}
		}()
	}

	for i := 0; i < j; i++ {
		<-sem
	}

	fmt.Fprintf(os.Stdout, "done.\n")
}

// Minimize trip IDs
func (minimizer IDMinimizer) minimizeTripIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := addFldsRenamers{newAddFldsRenamer(feed.TripsAddFlds), newAddFldsRenamer(feed.StopTimesAddFlds), newAddFldsRenamer(feed.FrequenciesAddFlds)}

	newMap := make(map[string]*gtfs.Trip)
	for _, t := range feed.Trips {
		oldId := t.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		t.Id = newId
		idCount = idCount + 1
		newMap[t.Id] = t

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Trips = newMap
	flds.apply()
}

// Minimize shape IDs
func (minimizer IDMinimizer) minimizeShapeIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := newAddFldsRenamer(feed.ShapesAddFlds)

	newMap := make(map[string]*gtfs.Shape)
	for _, s := range feed.Shapes {
		oldId := s.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		s.Id = newId
		idCount = idCount + 1
		newMap[s.Id] = s

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Shapes = newMap
	flds.apply()
}

// Minimize route IDs
func (minimizer IDMinimizer) minimizeRouteIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := newAddFldsRenamer(feed.RoutesAddFlds)

	newMap := make(map[string]*gtfs.Route)
	for _, r := range feed.Routes {
		oldId := r.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		r.Id = newId
		idCount = idCount + 1
		newMap[r.Id] = r

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Routes = newMap
	flds.apply()
}

// Minimize service IDs
func (minimizer IDMinimizer) minimizeServiceIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Service)
	for _, s := range feed.Services {
		s.SetId(minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base))
		idCount = idCount + 1
		newMap[s.Id()] = s
	}

	feed.Services = newMap
}

// Minimize stop IDs
func (minimizer IDMinimizer) minimizeStopIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := newAddFldsRenamer(feed.StopsAddFlds)

	newMap := make(map[string]*gtfs.Stop)
	for _, s := range feed.Stops {
		oldId := s.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		s.Id = newId
		idCount = idCount + 1
		newMap[s.Id] = s

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Stops = newMap
	flds.apply()
}

// Minimize attribution IDs
func (minimizer IDMinimizer) minimizeAttributionIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	for i := range feed.Attributions {
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		feed.Attributions[i].Id = newId
		idCount = idCount + 1
	}

	for _, ag := range feed.Agencies {
		for i := range ag.Attributions {
			newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
			ag.Attributions[i].Id = newId
			idCount = idCount + 1
		}
	}

	for _, r := range feed.Routes {
		for i := range r.Attributions {
			newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
			r.Attributions[i].Id = newId
			idCount = idCount + 1
		}
	}

	for _, t := range feed.Trips {
		if t.Attributions == nil {
			continue
		}
		for i := range *t.Attributions {
			newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
			(*t.Attributions)[i].Id = newId
			idCount = idCount + 1
		}
	}
}

// Minimize agency IDs
func (minimizer IDMinimizer) minimizeAgencyIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := newAddFldsRenamer(feed.AgenciesAddFlds)

	newMap := make(map[string]*gtfs.Agency)
	for _, a := range feed.Agencies {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Agencies = newMap
	flds.apply()
}

// Minimize fare IDs
func (minimizer IDMinimizer) minimizeFareIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := addFldsRenamers{newAddFldsRenamer(feed.FareAttributesAddFlds), newAddFldsRenamer(feed.FareRulesAddFlds)}

	newMap := make(map[string]*gtfs.FareAttribute)
	for _, a := range feed.FareAttributes {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.FareAttributes = newMap
	flds.apply()
}

// Minimize pathway IDs
func (minimizer IDMinimizer) minimizePathwayIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := newAddFldsRenamer(feed.PathwaysAddFlds)

	newMap := make(map[string]*gtfs.Pathway)
	for _, a := range feed.Pathways {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Pathways = newMap
	flds.apply()
}

// Minimize level IDs
func (minimizer IDMinimizer) minimizeLevelIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	flds := newAddFldsRenamer(feed.LevelsAddFlds)

	newMap := make(map[string]*gtfs.Level)
	for _, a := range feed.Levels {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		flds.rename(oldId, newId)
	}

	feed.Levels = newMap
	flds.apply()
}
