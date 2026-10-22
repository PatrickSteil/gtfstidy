// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// If the environment variable is set, the test runs main with the arguments of
// the other variable instead of testing something.
const (
	runMainEnv  = "GTFSTIDY_TEST_RUN_MAIN"
	mainArgsEnv = "GTFSTIDY_TEST_MAIN_ARGS"
)

// Errors that are reported with a panic in main have to make gtfstidy exit with a
// non-zero status.
func TestExitStatusOnInvalidArguments(t *testing.T) {
	if os.Getenv(runMainEnv) == "1" {
		os.Args = append([]string{"gtfstidy"}, strings.Fields(os.Getenv(mainArgsEnv))...)
		main()
		return
	}

	for _, args := range []string{"--date-start garbage feed", "--date-end 20251301 feed", "--keep-mots abc feed"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestExitStatusOnInvalidArguments$")
		cmd.Env = append(os.Environ(), runMainEnv+"=1", mainArgsEnv+"="+args)

		out, err := cmd.CombinedOutput()

		exitErr, ok := err.(*exec.ExitError)
		if !ok || exitErr.ExitCode() == 0 {
			t.Errorf("gtfstidy %s: expected a non-zero exit status, got error %v", args, err)
		}

		if !strings.Contains(string(out), "Error:") {
			t.Errorf("gtfstidy %s: expected an error message, got %q", args, out)
		}
	}
}
