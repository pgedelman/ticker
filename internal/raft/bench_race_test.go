//go:build race

package raft

// benchRaceEnabled reports whether the test binary was built with -race.
// The benchmark harness refuses to run under the race detector because it
// slows the code down by an order of magnitude and would distort the timings.
const benchRaceEnabled = true
