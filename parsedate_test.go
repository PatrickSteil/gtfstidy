// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/patrickbr/gtfsparser/gtfs"
)

// parseDateError returns the message of the panic caused by an invalid date.
func parseDateError(str string) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()

	parseDate(str)

	return ""
}

func TestParseDate(t *testing.T) {
	if got, want := parseDate("20260115"), gtfs.NewDate(15, 1, 2026); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

// If more than one part of the date is invalid, the first invalid part is reported.
func TestParseDateReportsFirstError(t *testing.T) {
	tests := []struct {
		date string
		want string
	}{
		{"2026011", "only has 7 characters"},
		{"20260132", "day must be"},
		{"20260015", "month must be"},
		{"20261315", "month must be"},
		{"18990115", "date must be"},
		{"20251345", "day must be"},
		{"21561301", "month must be"},
		{"21560145", "day must be"},
	}

	for _, test := range tests {
		if msg := parseDateError(test.date); !strings.Contains(msg, test.want) {
			t.Errorf("parseDate(%q): expected an error containing %q, got %q", test.date, test.want, msg)
		}
	}
}
