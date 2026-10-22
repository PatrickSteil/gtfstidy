// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"reflect"
	"testing"
)

func TestParseCoords(t *testing.T) {
	// the coordinates are given as latitude, longitude, the result is x, y
	want := [][2]float64{{11.5, 48.1}, {11.6, 48.2}}

	inputs := []string{
		"48.1,11.5,48.2,11.6",
		"48.1, 11.5,\n48.2, 11.6\n",
		"48.1,11.5,\r\n48.2,11.6\r\n",
		"\t48.1,\t11.5,\t48.2,\t11.6",
	}

	for _, in := range inputs {
		got, err := parseCoords(in)
		if err != nil {
			t.Errorf("parseCoords(%q): unexpected error %v", in, err)
		} else if !reflect.DeepEqual(got, want) {
			t.Errorf("parseCoords(%q) = %v, expected %v", in, got, want)
		}
	}
}

func TestParseCoordsErrors(t *testing.T) {
	for _, in := range []string{"48.1,11.5,48.2", "48.1,abc", "48.1,11.5,,"} {
		if _, err := parseCoords(in); err == nil {
			t.Errorf("parseCoords(%q): expected an error", in)
		}
	}
}
