package processors

import "unsafe"

// https://dev.to/chigbeef_77/bool-int-but-stupid-in-go-3jb3
func fastBoolConv(b bool) int {
	return int(*(*byte)(unsafe.Pointer(&b)))
}

// Implement a minimal geohash encoder (standard Base32 alphabet: "0123456789bcdefghjkmnpqrstuvwxyz")
var _base32 = []byte("0123456789bcdefghjkmnpqrstuvwxyz")

func geohash(lat, lon float64, precision int) string {
	latInterval := [2]float64{-90.0, 90.0}
	lonInterval := [2]float64{-180.0, 180.0}
	isEven := true
	bit := 0
	ch := 0
	hash := make([]byte, 0, precision)
	for len(hash) < precision {
		var mid float64
		if isEven {
			mid = (lonInterval[0] + lonInterval[1]) / 2
			breakLon := fastBoolConv(lon > mid)
			ch = (ch << 1) | breakLon
			lonInterval[1-breakLon] = mid
		} else {
			mid = (latInterval[0] + latInterval[1]) / 2
			breakLat := fastBoolConv(lat > mid)
			ch = (ch << 1) | breakLat
			latInterval[1-breakLat] = mid
		}
		isEven = !isEven
		bit++
		if bit == 5 {
			hash = append(hash, _base32[ch])
			bit = 0
			ch = 0
		}
	}
	return string(hash)
}
