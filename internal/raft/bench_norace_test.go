//go:build !race

package raft

// benchRaceEnabled reports whether the test binary was built with -race.
const benchRaceEnabled = false
