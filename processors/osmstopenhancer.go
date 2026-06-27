package processors

// OSMStopEnhancer enriches GTFS stops with data from an OpenStreetMap PBF file.
//
// Matching pipeline:
//  1. Load all OSM transit nodes from the PBF file (single pass).
//  2. Build a k-d tree over the nodes in O(M log M).
//  3. Match each GTFS stop in parallel (one goroutine per CPU):
//       a. k-d tree radius query in O(log M) — returns a small candidate set.
//       b. Re-score each candidate with Haversine + trigram name similarity.
//       c. Apply enrichment if the best score clears MinScore.
//
// Fields applied (non-destructively — only if currently empty/zero):
//   - Platform_code       ← OSM local_ref / ref
//   - Wheelchair_boarding ← OSM wheelchair (yes/limited→1, no→2)
//   - Desc                ← "shelter=yes bench=no tactile_paving=yes" etc.
//
// Optionally (FixCoordinates), the GTFS stop's lat/lon can also be snapped to
// the matched OSM node's coordinates, but only for tight matches — see
// FixCoordMaxMeters.

import (
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
	"github.com/qedus/osmpbf"
)

// OSMStopEnhancer is the processor struct.
type OSMStopEnhancer struct {
	// PBFFile is the path to the *.osm.pbf file to read.
	PBFFile string

	// MaxDistMeters is the maximum Haversine distance (metres) for a candidate
	// to be considered. Defaults to 150 m if zero.
	MaxDistMeters float64

	// MinScore is the minimum combined match score [0,1] required before any
	// enrichment is applied. Defaults to 0.25 if zero.
	MinScore float64

	// FixCoordinates, if true, overwrites the GTFS stop's lat/lon with the
	// matched OSM node's coordinates — but only when the match distance is
	// within FixCoordMaxMeters. This is deliberately a tighter bar than
	// MinScore/MaxDistMeters: a match good enough to add a Desc tag is not
	// necessarily good enough to silently relocate a stop.
	FixCoordinates bool

	// FixCoordMaxMeters caps how far (in metres) a stop may be moved when
	// FixCoordinates is set. Defaults to 30 m if zero.
	FixCoordMaxMeters float64

	// DryRun logs what would change without modifying the feed.
	DryRun bool

	// Verbose enables per-stop match logging.
	Verbose bool
}

// osmStop is an OSM node that passed the public-transport tag filter.
type osmStop struct {
	id         int64
	lat, lon   float64
	name       string
	nameTrig   map[string]struct{} // precomputed trigram set for name
	ref        string              // platform code (local_ref or ref)
	wheelchair string              // yes / limited / no
	shelter    string              // yes / no
	bench      string              // yes / no
	tactile    string              // yes / no
}

// ---------------------------------------------------------------------------
// Run — implements the gtfstidy Processor interface
// ---------------------------------------------------------------------------

func (pro OSMStopEnhancer) Run(feed *gtfsparser.Feed) {
	if pro.MaxDistMeters == 0 {
		pro.MaxDistMeters = 150
	}
	if pro.MinScore == 0 {
		pro.MinScore = 0.25
	}
	if pro.FixCoordMaxMeters == 0 {
		pro.FixCoordMaxMeters = 30
	}

	fmt.Fprintf(os.Stdout, "OSMStopEnhancer: loading %s ... ", pro.PBFFile)

	nodes, err := loadOSMStops(pro.PBFFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nOSMStopEnhancer: failed to read PBF: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stdout, "loaded %d transit nodes. Building k-d tree ... ", len(nodes))

	tree := buildKDTree(nodes)
	fmt.Fprintf(os.Stdout, "done.\n")

	// Collect stops into a slice for parallel work dispatch.
	stops := make([]*gtfs.Stop, 0, len(feed.Stops))
	for _, s := range feed.Stops {
		stops = append(stops, s)
	}

	// matchResult carries the outcome for a single stop back to the main goroutine.
	type matchResult struct {
		stop  *gtfs.Stop
		match *osmStop
		score float64
		dist  float64
	}

	results := make([]matchResult, len(stops))

	// Fan out over GOMAXPROCS workers.
	numWorkers := runtime.GOMAXPROCS(-1)
	chunkSize := (len(stops) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		lo := w * chunkSize
		hi := lo + chunkSize
		if hi > len(stops) {
			hi = len(stops)
		}
		if lo >= hi {
			break
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				s := stops[i]
				match, score := bestMatchKD(
					float64(s.Lat), float64(s.Lon), s.Name,
					tree, pro.MaxDistMeters, pro.MinScore,
				)
				dist := 0.0
				if match != nil {
					dist = haversineM(float64(s.Lat), float64(s.Lon), match.lat, match.lon)
				}
				results[i] = matchResult{stop: s, match: match, score: score, dist: dist}
			}
		}(lo, hi)
	}
	wg.Wait()

	// Apply results on the main goroutine (no lock needed — each result owns
	// its stop pointer and we're done with workers).
	//
	// claimedBy tracks which GTFS stops have already been matched to a given
	// OSM node ID, so we can warn about many-to-one collisions (e.g. two
	// near-duplicate GTFS stops both snapping to the same real-world platform).
	claimedBy := make(map[int64][]*gtfs.Stop)

	var nPlatform, nWheelchair, nDesc, nCoords int

	for _, r := range results {
		if r.match == nil {
			continue
		}
		if pro.Verbose {
			willFix := pro.FixCoordinates && r.dist <= pro.FixCoordMaxMeters
			tag := ""
			if willFix {
				tag = " [coords fixed]"
			}
			fmt.Fprintf(os.Stdout, "  [score=%.3f dist=%.0fm]%s GTFS %q ← OSM %q\n",
				r.score, r.dist, tag, r.stop.Name, r.match.name)
		}

		if !pro.DryRun {
			res := applyToStop(r.stop, r.match, r.dist, pro.FixCoordinates, pro.FixCoordMaxMeters)
			if res.platformSet {
				nPlatform++
			}
			if res.wheelchairSet {
				nWheelchair++
			}
			if res.descSet {
				nDesc++
			}
			if res.coordsFixed {
				nCoords++
			}
		}

		claimedBy[r.match.id] = append(claimedBy[r.match.id], r.stop)
	}

	// Warn about collisions: multiple GTFS stops matched to the same OSM node.
	for osmID, gtfsStops := range claimedBy {
		if len(gtfsStops) < 2 {
			continue
		}
		names := make([]string, len(gtfsStops))
		for i, s := range gtfsStops {
			names[i] = fmt.Sprintf("%q (id=%s)", s.Name, s.Id)
		}
		fmt.Fprintf(os.Stderr,
			"OSMStopEnhancer: warning: OSM node %d matched by %d GTFS stops: %s\n",
			osmID, len(gtfsStops), strings.Join(names, ", "))
	}

	action := "enhanced"
	if pro.DryRun {
		action = "would enhance"
	}
	enhanced := 0
	for _, gs := range claimedBy {
		enhanced += len(gs)
	}
	fmt.Fprintf(os.Stdout,
		"OSMStopEnhancer: %s %d / %d stops "+
			"(platform_code: %d, wheelchair: %d, desc: %d, coords fixed: %d).\n",
		action, enhanced, len(feed.Stops), nPlatform, nWheelchair, nDesc, nCoords)
}

// ---------------------------------------------------------------------------
// Enrichment
// ---------------------------------------------------------------------------

// applyResult reports exactly which fields were touched, so callers can keep
// per-field statistics (useful for dry-run summaries).
type applyResult struct {
	platformSet   bool
	wheelchairSet bool
	descSet       bool
	coordsFixed   bool
}

func applyToStop(stop *gtfs.Stop, m *osmStop, dist float64, fixCoords bool, fixCoordMaxM float64) applyResult {
	var res applyResult

	if m.ref != "" && stop.Platform_code == "" {
		stop.Platform_code = m.ref
		res.platformSet = true
	}
	if stop.Wheelchair_boarding == 0 {
		if wb := osmWheelchair(m.wheelchair); wb != 0 {
			stop.Wheelchair_boarding = wb
			res.wheelchairSet = true
		}
	}
	if stop.Desc == "" {
		if note := amenityNote(m); note != "" {
			stop.Desc = note
			res.descSet = true
		}
	}
	if fixCoords && dist <= fixCoordMaxM {
		stop.Lat = float32(m.lat)
		stop.Lon = float32(m.lon)
		res.coordsFixed = true
	}

	return res
}

func osmWheelchair(v string) int8 {
	switch v {
	case "yes", "designated", "limited":
		return 1
	case "no":
		return 2
	}
	return 0
}

func amenityNote(m *osmStop) string {
	var parts []string
	for _, kv := range []struct{ k, v string }{
		{"shelter", m.shelter},
		{"bench", m.bench},
		{"tactile_paving", m.tactile},
	} {
		if kv.v != "" {
			parts = append(parts, kv.k+"="+kv.v)
		}
	}
	return strings.Join(parts, " ")
}

// ---------------------------------------------------------------------------
// OSM PBF loading
// ---------------------------------------------------------------------------

func isTransitNode(tags map[string]string) bool {
	switch tags["public_transport"] {
	case "stop_position", "platform":
		return true
	}
	switch tags["highway"] {
	case "bus_stop":
		return true
	}
	switch tags["railway"] {
	case "stop", "halt", "tram_stop":
		return true
	}
	return false
}

func firstTag(tags map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := tags[k]; v != "" {
			return v
		}
	}
	return ""
}

func loadOSMStops(path string) ([]osmStop, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := osmpbf.NewDecoder(f)
	dec.SetBufferSize(osmpbf.MaxBlobSize)
	if err := dec.Start(runtime.GOMAXPROCS(-1)); err != nil {
		return nil, err
	}

	var out []osmStop
	for {
		obj, err := dec.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		node, ok := obj.(*osmpbf.Node)
		if !ok {
			continue
		}
		if !isTransitNode(node.Tags) {
			continue
		}
		name := firstTag(node.Tags, "name")
		out = append(out, osmStop{
			id:         node.ID,
			lat:        node.Lat,
			lon:        node.Lon,
			name:       name,
			nameTrig:   trigramSet(name),
			ref:        firstTag(node.Tags, "local_ref", "ref"),
			wheelchair: firstTag(node.Tags, "wheelchair"),
			shelter:    firstTag(node.Tags, "shelter"),
			bench:      firstTag(node.Tags, "bench"),
			tactile:    firstTag(node.Tags, "tactile_paving"),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// k-d tree
//
// We store lat/lon directly as the two dimensions. Within the small spatial
// scales relevant to stop matching (≤ 150 m), treating lat/lon as Euclidean
// introduces less than 0.01% error — fully acceptable. Haversine is used only
// for the final scoring pass on the small candidate set the tree returns.
//
// The tree is built by recursively choosing the median along the axis with the
// highest variance (standard split heuristic). Nodes are stored in a flat
// slice for cache friendliness; left/right children are indices into that
// slice (-1 = leaf).
// ---------------------------------------------------------------------------

type kdNode struct {
	stop        osmStop
	splitAxis   int // 0=lat, 1=lon
	left, right int // indices into kdTree.nodes; -1 = none
}

type kdTree struct {
	nodes []kdNode
}

func buildKDTree(stops []osmStop) *kdTree {
	if len(stops) == 0 {
		return &kdTree{}
	}
	// Work on index slices to avoid copying osmStop structs during sort.
	indices := make([]int, len(stops))
	for i := range indices {
		indices[i] = i
	}
	t := &kdTree{nodes: make([]kdNode, 0, len(stops))}
	t.build(stops, indices)
	return t
}

// chooseAxis picks the split axis with the higher variance among the given
// indices (0=lat, 1=lon). Using variance rather than alternating by depth
// keeps the tree balanced for data with strong directional structure, e.g.
// a long north-south rail corridor where almost all the spread is in lat.
func chooseAxis(stops []osmStop, indices []int) int {
	var sumLat, sumLon, sumLat2, sumLon2 float64
	n := float64(len(indices))
	for _, idx := range indices {
		s := stops[idx]
		sumLat += s.lat
		sumLon += s.lon
		sumLat2 += s.lat * s.lat
		sumLon2 += s.lon * s.lon
	}
	varLat := sumLat2/n - (sumLat/n)*(sumLat/n)
	varLon := sumLon2/n - (sumLon/n)*(sumLon/n)
	if varLon > varLat {
		return 1
	}
	return 0
}

// build recursively partitions indices and appends nodes; returns the index of
// the node it just inserted.
func (t *kdTree) build(stops []osmStop, indices []int) int {
	if len(indices) == 0 {
		return -1
	}

	// Choose the split axis with the higher variance over the current subset,
	// rather than blindly alternating by depth.
	axis := chooseAxis(stops, indices)

	// Sort by the chosen axis and pick the median.
	sort.Slice(indices, func(i, j int) bool {
		if axis == 0 {
			return stops[indices[i]].lat < stops[indices[j]].lat
		}
		return stops[indices[i]].lon < stops[indices[j]].lon
	})
	mid := len(indices) / 2

	// Reserve a slot before recursing so children can store absolute indices.
	myIdx := len(t.nodes)
	t.nodes = append(t.nodes, kdNode{
		stop:      stops[indices[mid]],
		splitAxis: axis,
		left:      -1,
		right:     -1,
	})

	t.nodes[myIdx].left = t.build(stops, indices[:mid])
	t.nodes[myIdx].right = t.build(stops, indices[mid+1:])
	return myIdx
}

// radiusSearch returns all osmStop pointers whose lat/lon distance from
// (lat, lon) is within radiusDegLat/radiusDegLon (an axis-aligned ellipse —
// see degreesForMetersLat/Lon). The caller applies Haversine for final
// scoring/trimming.
func (t *kdTree) radiusSearch(lat, lon, radiusDegLat, radiusDegLon float64) []*osmStop {
	if len(t.nodes) == 0 {
		return nil
	}
	var out []*osmStop
	t.search(0, lat, lon, radiusDegLat, radiusDegLon, &out)
	return out
}

func (t *kdTree) search(nodeIdx int, lat, lon, radiusDegLat, radiusDegLon float64, out *[]*osmStop) {
	if nodeIdx < 0 || nodeIdx >= len(t.nodes) {
		return
	}
	n := &t.nodes[nodeIdx]

	dLat := (n.stop.lat - lat) / radiusDegLat
	dLon := (n.stop.lon - lon) / radiusDegLon
	if dLat*dLat+dLon*dLon <= 1.0 {
		*out = append(*out, &n.stop)
	}

	// Determine which side of the split plane the query point is on, and the
	// radius (in degrees) relevant to that axis for the plane-crossing test.
	var diff, radiusDeg float64
	if n.splitAxis == 0 {
		diff = lat - n.stop.lat
		radiusDeg = radiusDegLat
	} else {
		diff = lon - n.stop.lon
		radiusDeg = radiusDegLon
	}

	// Always search the near side; search the far side only if the query
	// ellipse crosses the split plane.
	near, far := n.left, n.right
	if diff > 0 {
		near, far = n.right, n.left
	}
	t.search(near, lat, lon, radiusDegLat, radiusDegLon, out)
	if diff*diff <= radiusDeg*radiusDeg {
		t.search(far, lat, lon, radiusDegLat, radiusDegLon, out)
	}
}

const metersPerDegreeLat = 111_320.0

// degreesForMetersLat converts a metre radius to a degree radius along the
// latitude axis. Latitude degrees are ~constant length everywhere, so no
// correction is needed.
func degreesForMetersLat(meters float64) float64 {
	return meters / metersPerDegreeLat
}

// degreesForMetersLon converts a metre radius to a degree radius along the
// longitude axis at the given latitude. Longitude degrees shrink by cos(lat)
// as you move away from the equator, so without this correction a fixed
// degree-radius box becomes too *narrow* in longitude at high latitudes
// (the opposite problem from the old "equator over-estimate" framing) and
// can miss real candidates there. We clamp the cosine to a small epsilon to
// avoid blowing up near the poles.
func degreesForMetersLon(lat, meters float64) float64 {
	cosLat := math.Cos(lat * math.Pi / 180)
	if cosLat < 0.01 {
		cosLat = 0.01
	}
	return meters / (metersPerDegreeLat * cosLat)
}

// ---------------------------------------------------------------------------
// Matching with k-d tree
// ---------------------------------------------------------------------------

func bestMatchKD(
	lat, lon float64,
	name string,
	tree *kdTree,
	maxDist, minScore float64,
) (*osmStop, float64) {
	radiusDegLat := degreesForMetersLat(maxDist)
	radiusDegLon := degreesForMetersLon(lat, maxDist)
	candidates := tree.radiusSearch(lat, lon, radiusDegLat, radiusDegLon)

	// Precompute the GTFS stop's trigram set once, rather than re-deriving it
	// for every candidate inside the loop below.
	gtfsTrig := trigramSet(name)

	var best *osmStop
	bestScore := -1.0

	for _, c := range candidates {
		dist := haversineM(lat, lon, c.lat, c.lon)
		if dist > maxDist {
			// The degree-radius bounding ellipse is slightly wider than the
			// true Haversine circle, so trim the over-fetch here.
			continue
		}

		distScore := 1.0 - dist/maxDist
		nameScore := trigramSimilaritySets(gtfsTrig, c.nameTrig)
		score := 0.6*distScore + 0.4*nameScore

		if score > bestScore {
			bestScore = score
			best = c
		}
	}

	if bestScore < minScore {
		return nil, 0
	}
	return best, bestScore
}

// ---------------------------------------------------------------------------
// Haversine distance
// ---------------------------------------------------------------------------

const earthRadiusM = 6_371_000.0

func haversineM(lat1, lon1, lat2, lon2 float64) float64 {
	φ1 := lat1 * math.Pi / 180
	φ2 := lat2 * math.Pi / 180
	Δφ := (lat2 - lat1) * math.Pi / 180
	Δλ := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(Δφ/2)*math.Sin(Δφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*math.Sin(Δλ/2)*math.Sin(Δλ/2)
	return earthRadiusM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// ---------------------------------------------------------------------------
// Trigram similarity (Dice coefficient)
// ---------------------------------------------------------------------------

// trigramSet builds the set of 3-character shingles for s, after folding to
// lowercase, stripping diacritics, and dropping punctuation. Empty/short
// strings are padded with spaces so they still produce at least one trigram.
func trigramSet(s string) map[string]struct{} {
	s = normaliseString(s)
	runes := []rune(s)
	for len(runes) < 3 {
		runes = append(runes, ' ')
	}
	set := make(map[string]struct{}, len(runes)-2)
	for i := 0; i <= len(runes)-3; i++ {
		set[string(runes[i:i+3])] = struct{}{}
	}
	return set
}

// trigramSimilaritySets computes the Dice coefficient between two precomputed
// trigram sets. Use this (rather than trigramSimilarity) whenever one or both
// sides are reused across many comparisons, e.g. matching one GTFS stop name
// against many OSM candidates.
func trigramSimilaritySets(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	var shared int
	// Iterate the smaller set for fewer lookups.
	small, big := a, b
	if len(b) < len(a) {
		small, big = b, a
	}
	for t := range small {
		if _, ok := big[t]; ok {
			shared++
		}
	}
	return float64(2*shared) / float64(len(a)+len(b))
}

// trigramSimilarity is a convenience wrapper for one-off comparisons (kept
// for callers/tests that don't have precomputed sets).
func trigramSimilarity(a, b string) float64 {
	if a == "" || b == "" {
		return 0
	}
	return trigramSimilaritySets(trigramSet(a), trigramSet(b))
}

// diacriticFold maps common accented/special Latin letters to their plain
// ASCII equivalents. This is a hand-rolled substitute for a full Unicode
// NFD-normalize-and-strip-marks pipeline (which would require pulling in
// golang.org/x/text); it covers the characters that actually show up in
// German/French/etc. transit stop names, which is the common case here.
var diacriticFold = map[rune]string{
	'ä': "a", 'ö': "o", 'ü': "u", 'Ä': "a", 'Ö': "o", 'Ü': "u", 'ß': "ss",
	'à': "a", 'á': "a", 'â': "a", 'ã': "a", 'å': "a",
	'è': "e", 'é': "e", 'ê': "e", 'ë': "e",
	'ì': "i", 'í': "i", 'î': "i", 'ï': "i",
	'ò': "o", 'ó': "o", 'ô': "o", 'õ': "o",
	'ù': "u", 'ú': "u", 'û': "u",
	'ñ': "n", 'ç': "c", 'ý': "y", 'ÿ': "y",
	'À': "a", 'Á': "a", 'Â': "a", 'Ã': "a", 'Å': "a",
	'È': "e", 'É': "e", 'Ê': "e", 'Ë': "e",
	'Ì': "i", 'Í': "i", 'Î': "i", 'Ï': "i",
	'Ò': "o", 'Ó': "o", 'Ô': "o", 'Õ': "o",
	'Ù': "u", 'Ú': "u", 'Û': "u",
	'Ñ': "n", 'Ç': "c", 'Ý': "y",
}

func normaliseString(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if folded, ok := diacriticFold[r]; ok {
			b.WriteString(folded)
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}
