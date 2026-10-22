// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"os"
	"runtime/debug"
	"testing"
)

func TestConfigureGCWithoutGOGC(t *testing.T) {
	if old, ok := os.LookupEnv("GOGC"); ok {
		os.Unsetenv("GOGC")
		defer os.Setenv("GOGC", old)
	}

	defer debug.SetGCPercent(debug.SetGCPercent(100))

	configureGC()

	if got := debug.SetGCPercent(100); got != defaultGCPercent {
		t.Errorf("expected the GC percentage to be %d, got %d", defaultGCPercent, got)
	}
}

// A GOGC set by the user takes precedence.
func TestConfigureGCWithGOGC(t *testing.T) {
	t.Setenv("GOGC", "200")

	defer debug.SetGCPercent(debug.SetGCPercent(77))

	configureGC()

	if got := debug.SetGCPercent(100); got != 77 {
		t.Errorf("expected the GC percentage to stay 77, got %d", got)
	}
}
