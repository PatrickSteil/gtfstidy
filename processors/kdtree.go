// Copyright 2025 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
	"sort"
)

// Earth radius in kilometers
const EarthRadiusKm = 6371.0

// Point holds lat/lon and a generic payload of type T
type Point[T any] struct {
	Lat, Lon float64
	Data     T
}

// Node in a KD-tree
type Node[T any] struct {
	Point Point[T]
	Left  *Node[T]
	Right *Node[T]
	Axis  int // 0 = lat, 1 = lon
}

// BuildKDTree builds a balanced KD-tree from points slice
func BuildKDTree[T any](points []Point[T], depth int) *Node[T] {
	if len(points) == 0 {
		return nil
	}

	axis := depth % 2

	// Sort points by current axis (lat or lon)
	sort.SliceStable(points, func(i, j int) bool {
		if axis == 0 {
			return points[i].Lat < points[j].Lat
		}
		return points[i].Lon < points[j].Lon
	})

	median := len(points) / 2

	node := &Node[T]{
		Point: points[median],
		Axis:  axis,
	}

	node.Left = BuildKDTree(points[:median], depth+1)
	node.Right = BuildKDTree(points[median+1:], depth+1)

	return node
}

// Insert adds a new point to the tree
func Insert[T any](root *Node[T], point Point[T], depth int) *Node[T] {
	if root == nil {
		return &Node[T]{Point: point, Axis: depth % 2}
	}

	var key, rootKey float64
	if root.Axis == 0 {
		key, rootKey = point.Lat, root.Point.Lat
	} else {
		key, rootKey = point.Lon, root.Point.Lon
	}

	if key < rootKey {
		root.Left = Insert(root.Left, point, depth+1)
	} else {
		root.Right = Insert(root.Right, point, depth+1)
	}

	return root
}

// Haversine distance in km between two lat/lon points
func Haversine(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return EarthRadiusKm * c
}

func latLonBoundingBox(lat, lon, radiusKm float64) (minLat, maxLat, minLon, maxLon float64) {
	dLat := (radiusKm / EarthRadiusKm) * (180.0 / math.Pi)
	dLon := dLat / math.Cos(lat*math.Pi/180.0)

	return lat - dLat, lat + dLat, lon - dLon, lon + dLon
}

func coordDegrees(radiusKm, lat float64, cd int) float64 {
	rad := radiusKm / EarthRadiusKm * (180.0 / math.Pi)
	if cd == 0 {
		return rad // latitude
	}
	return rad / math.Cos(lat*math.Pi/180.0) // longitude
}

func SearchRange[T any](node *Node[T], query Point[T], radiusKm float64, depth int, results *[]Point[T]) {
	if node == nil {
		return
	}

	distance := Haversine(query.Lat, query.Lon, node.Point.Lat, node.Point.Lon)
	if distance <= radiusKm {
		*results = append(*results, node.Point)
	}

	cd := depth % 2 // 0 = lat, 1 = lon

	var queryCoord, nodeCoord, delta float64
	if cd == 0 {
		queryCoord = query.Lat
		nodeCoord = node.Point.Lat
	} else {
		queryCoord = query.Lon
		nodeCoord = node.Point.Lon
	}

	delta = coordDegrees(radiusKm, query.Lat, cd)

	// Search left subtree if it may contain points within radius
	if queryCoord-delta <= nodeCoord {
		SearchRange(node.Left, query, radiusKm, depth+1, results)
	}
	// Search right subtree if it may contain points within radius
	if queryCoord+delta >= nodeCoord {
		SearchRange(node.Right, query, radiusKm, depth+1, results)
	}
}
