// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"math"

	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

// StopClusterIdx stores objects for fast nearest-neighbor retrieval.
// The grid is stored as a flat 1D slice (index = x*yHeight + y) to
// avoid double-indirection and improve cache locality.
// Each occupied cell holds a small []int32 of cluster IDs, avoiding
// the per-cell map overhead of the previous map[int]bool layout.
type StopClusterIdx struct {
	width      float64
	height     float64
	cellWidth  float64
	cellHeight float64
	xWidth     uint
	yHeight    uint
	llx        float64
	lly        float64
	urx        float64
	ury        float64
	grid       [][]int32 // flat 1D: index = x*yHeight + y
}

func getStopLatLon(s *gtfs.Stop) (float32, float32) {
	if math.IsNaN(float64(s.Lat)) || math.IsNaN(float64(s.Lon)) {
		// child came from an object with optional lat/lon,
		// in which case the standard guarantees a parent with lat/lon
		if s.Parent_station != nil {
			if math.IsNaN(float64(s.Parent_station.Lat)) || math.IsNaN(float64(s.Parent_station.Lon)) {
				panic(fmt.Errorf("Could not find lat/lon coordinate for stop %s", s.Id))
			}
			return s.Parent_station.Lat, s.Parent_station.Lon
		}
		panic(fmt.Errorf("Could not find lat/lon coordinate for stop %s", s.Id))
	}
	return s.Lat, s.Lon
}

func NewStopClusterIdx(clusters []*StopCluster, cellWidth, cellHeight float64) *StopClusterIdx {
	idx := StopClusterIdx{
		width:      0.0,
		height:     0.0,
		cellWidth:  cellWidth,
		cellHeight: cellHeight,
		xWidth:     0,
		yHeight:    0,
		llx:        math.Inf(1),
		lly:        math.Inf(1),
		urx:        math.Inf(-1),
		ury:        math.Inf(-1),
	}

	for _, cluster := range clusters {
		for _, s := range cluster.Parents {
			x, y := latLngToWebMerc(getStopLatLon(s))
			if x < idx.llx {
				idx.llx = x
			}
			if x > idx.urx {
				idx.urx = x
			}
			if y < idx.lly {
				idx.lly = y
			}
			if y > idx.ury {
				idx.ury = y
			}
		}

		for _, s := range cluster.Childs {
			x, y := latLngToWebMerc(getStopLatLon(s))
			if x < idx.llx {
				idx.llx = x
			}
			if x > idx.urx {
				idx.urx = x
			}
			if y < idx.lly {
				idx.lly = y
			}
			if y > idx.ury {
				idx.ury = y
			}
		}
	}

	idx.width = idx.urx - idx.llx
	idx.height = idx.ury - idx.lly

	if idx.width < 0 || idx.height < 0 {
		idx.width = 0
		idx.height = 0
		idx.xWidth = 0
		idx.yHeight = 0
		return &idx
	}

	idx.xWidth = uint(math.Ceil(idx.width / idx.cellWidth))
	idx.yHeight = uint(math.Ceil(idx.height / idx.cellHeight))

	// In case we got one coodinate, the width and height should be > 0
	if idx.xWidth == 0 {
		idx.xWidth = 1
	}
	if idx.yHeight == 0 {
		idx.yHeight = 1
	}

	idx.grid = make([][]int32, idx.xWidth*idx.yHeight)

	for cid, cluster := range clusters {
		for _, s := range cluster.Parents {
			lat, lon := getStopLatLon(s)
			idx.Add(float64(lat), float64(lon), int32(cid))
		}
		for _, s := range cluster.Childs {
			lat, lon := getStopLatLon(s)
			idx.Add(float64(lat), float64(lon), int32(cid))
		}
	}

	return &idx
}

func (gi *StopClusterIdx) cellIndex(x, y uint) uint {
	return x*gi.yHeight + y
}

func (gi *StopClusterIdx) Add(lat float64, lon float64, obj int32) {
	lx, ly := latLngToWebMerc(float32(lat), float32(lon))

	x := gi.getCellXFromX(lx)
	y := gi.getCellYFromY(ly)

	if x >= gi.xWidth || y >= gi.yHeight {
		return
	}

	idx := gi.cellIndex(x, y)
	gi.grid[idx] = append(gi.grid[idx], obj)
}

func (gi *StopClusterIdx) GetNeighbors(excludeCid int, c *StopCluster, d float64) map[int]bool {
	ret := make(map[int]bool)

	// empty or degenerate index
	if gi.xWidth == 0 || gi.yHeight == 0 || len(gi.grid) == 0 {
		return ret
	}

	for _, st := range c.Parents {
		lat, lon := getStopLatLon(st)
		neighs := gi.GetNeighborsByLatLon(float64(lat), float64(lon), d)
		for cid := range neighs {
			if cid == excludeCid {
				continue
			}
			ret[cid] = true
		}
	}

	for _, st := range c.Childs {
		lat, lon := getStopLatLon(st)
		neighs := gi.GetNeighborsByLatLon(float64(lat), float64(lon), d)
		for cid := range neighs {
			if cid == excludeCid {
				continue
			}
			ret[cid] = true
		}
	}
	return ret
}

func (gi *StopClusterIdx) GetNeighborsByLatLon(lat float64, lon float64, d float64) map[int]bool {
	ret := make(map[int]bool)

	// empty or degenerate index
	if gi.xWidth == 0 || gi.yHeight == 0 || len(gi.grid) == 0 {
		return ret
	}

	xPerm := uint(math.Ceil(d / gi.cellWidth))
	yPerm := uint(math.Ceil(d / gi.cellHeight))

	lx, ly := latLngToWebMerc(float32(lat), float32(lon))

	cx := gi.getCellXFromX(lx)
	cy := gi.getCellYFromY(ly)

	var swX, swY uint
	if cx >= xPerm {
		swX = cx - xPerm
	}
	if cy >= yPerm {
		swY = cy - yPerm
	}

	neX := cx + xPerm
	if neX >= gi.xWidth {
		neX = gi.xWidth - 1
	}
	neY := cy + yPerm
	if neY >= gi.yHeight {
		neY = gi.yHeight - 1
	}

	for x := swX; x <= neX; x++ {
		for y := swY; y <= neY; y++ {
			for _, cid := range gi.grid[gi.cellIndex(x, y)] {
				ret[int(cid)] = true
			}
		}
	}

	return ret
}

func (gi *StopClusterIdx) getCellXFromX(x float64) uint {
	return uint(math.Floor(math.Max(0, x-gi.llx) / gi.cellWidth))
}

func (gi *StopClusterIdx) getCellYFromY(y float64) uint {
	return uint(math.Floor(math.Max(0, y-gi.lly) / gi.cellHeight))
}
