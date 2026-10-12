package raft

// Benchmark harness for election time, commit latency and throughput.
//
// It is gated behind RAFT_BENCH=1 so a normal `go test ./...` skips it:
//
//	RAFT_BENCH=1 go test -run TestBench -v -timeout 60m ./internal/raft
//
// Optional overrides: RAFT_BENCH_TRIALS (election trials per cluster size,
// default 300), RAFT_BENCH_COMMITS (sequential commits per cluster size,
// default 2000), RAFT_BENCH_N (entries per throughput run, default 200000),
// RAFT_BENCH_RUNS (throughput runs per cluster size, default 5).
//
// Raw per-trial results go to bench-results/*.csv and a summary to
// BENCHMARKS.md, both at the repository root.

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

var benchClusterSizes = []int{3, 5, 7}

const (
	benchElectionTimeout = 5 * time.Second
	benchCommitTimeout   = 5 * time.Second
	benchThroughputWait  = 120 * time.Second
	benchConvergeWait    = 10 * time.Second
	benchSettle          = 200 * time.Millisecond
	benchElectionPoll    = 1 * time.Millisecond
	benchCommitPoll      = 100 * time.Microsecond
	benchCommandSize     = 64
)

// benchConfig holds the knobs that can be overridden from the environment.
type benchConfig struct {
	trials  int
	commits int
	n       int
	runs    int
}

func benchEnvInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q: expected a positive integer", name, v)
	}
	return n
}

// benchStats is the summary of one set of latency samples.
type benchStats struct {
	trials   int
	ok       int
	failures int
	min      float64
	p50      float64
	p95      float64
	p99      float64
	max      float64
}

// benchPercentile returns the nearest-rank percentile of sorted samples.
func benchPercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = max(rank, 1)
	return sorted[rank-1]
}

func benchSummarize(samples []float64, failures int) benchStats {
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	s := benchStats{trials: len(samples) + failures, ok: len(samples), failures: failures}
	if len(sorted) == 0 {
		s.min, s.p50, s.p95, s.p99, s.max = math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()
		return s
	}
	s.min = sorted[0]
	s.p50 = benchPercentile(sorted, 50)
	s.p95 = benchPercentile(sorted, 95)
	s.p99 = benchPercentile(sorted, 99)
	s.max = sorted[len(sorted)-1]
	return s
}

func benchMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func benchFmt(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.2f", v)
}

// benchFirstLeader returns the first node in nodes that is Leader in a term
// greater than minTerm, or nil.
func benchFirstLeader(nodes []*Node, minTerm int) (*Node, int) {
	for _, node := range nodes {
		if _, role, term, _, _ := node.Snapshot(); role == Leader && term > minTerm {
			return node, term
		}
	}
	return nil, 0
}

// benchAwaitLeader polls nodes every poll interval until one of them is
// Leader in a term greater than minTerm. elapsed is measured from start.
func benchAwaitLeader(nodes []*Node, minTerm int, start time.Time, timeout time.Duration) (leader *Node, term int, elapsed time.Duration, ok bool) {
	for {
		if leader, term = benchFirstLeader(nodes, minTerm); leader != nil {
			return leader, term, time.Since(start), true
		}
		if time.Since(start) > timeout {
			return nil, 0, time.Since(start), false
		}
		time.Sleep(benchElectionPoll)
	}
}

// benchKillAll kills every node so heartbeat tickers and election timers stop.
// Kill can race with an election timer that has already fired (the node
// becomes a candidate again after being marked Dead), so it re-checks and
// re-kills a few times. It returns how many nodes were still alive after the
// final pass; the caller reports that rather than hiding it.
func benchKillAll(nodes []*Node) int {
	for range 3 {
		for _, node := range nodes {
			node.Kill()
		}
		time.Sleep(2 * time.Millisecond)
		alive := 0
		for _, node := range nodes {
			if _, role, _, _, _ := node.Snapshot(); role != Dead {
				alive++
			}
		}
		if alive == 0 {
			return 0
		}
	}
	alive := 0
	for _, node := range nodes {
		if _, role, _, _, _ := node.Snapshot(); role != Dead {
			alive++
		}
	}
	return alive
}

// benchReleaseLogs drops the logs of dead nodes after a throughput run.
// A killed node's Start goroutine keeps it reachable forever, so without this
// every 200k-entry run would stay on the heap and later runs would pay for it
// in GC time. It only releases a node whose apply loop has caught up, so the
// updateLogs goroutine never indexes into the dropped slice.
func benchReleaseLogs(nodes []*Node) {
	for _, node := range nodes {
		node.mu.Lock()
		if node.role == Dead && node.lastApplied >= node.commitIndex {
			node.logs = nil
		}
		node.mu.Unlock()
	}
}

// benchStableLeader starts a fresh cluster, waits for a leader and lets
// heartbeats run for benchSettle so the cluster is in steady state.
func benchStableLeader(size int) ([]*Node, *Node, error) {
	nodes := startCluster(size)
	leader, _, _, ok := benchAwaitLeader(nodes, 0, time.Now(), benchElectionTimeout)
	if !ok {
		benchKillAll(nodes)
		return nil, nil, fmt.Errorf("no leader within %v", benchElectionTimeout)
	}
	time.Sleep(benchSettle)
	if _, role, _, _, _ := leader.Snapshot(); role != Leader {
		benchKillAll(nodes)
		return nil, nil, fmt.Errorf("leader lost leadership during the %v settle period", benchSettle)
	}
	return nodes, leader, nil
}

// benchCommand returns a 64-byte command that encodes its sequence number, so
// log comparisons can tell entries apart.
func benchCommand(buf []byte, seq int) []byte {
	cmd := buf[:benchCommandSize:benchCommandSize]
	binary.BigEndian.PutUint64(cmd, uint64(seq))
	for i := 8; i < benchCommandSize; i++ {
		cmd[i] = byte(seq + i)
	}
	return cmd
}

// benchWriter collects CSV rows for one result file.
type benchWriter struct {
	header []string
	rows   [][]string
}

func (w *benchWriter) add(fields ...any) {
	row := make([]string, len(fields))
	for i, f := range fields {
		switch v := f.(type) {
		case float64:
			row[i] = strconv.FormatFloat(v, 'f', 4, 64)
		default:
			row[i] = fmt.Sprint(v)
		}
	}
	w.rows = append(w.rows, row)
}

func (w *benchWriter) save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	cw := csv.NewWriter(f)
	cw.Write(w.header)
	cw.WriteAll(w.rows)
	if err := cw.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func benchRepoRoot() string {
	if out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		return strings.TrimSpace(string(out))
	}
	return filepath.Join("..", "..")
}

func benchGitCommit() string {
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	commit := strings.TrimSpace(string(out))
	if status, err := exec.Command("git", "status", "--porcelain").Output(); err == nil && len(bytes.TrimSpace(status)) > 0 {
		commit += " (working tree has uncommitted changes)"
	}
	return commit
}

func benchCPUModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if name, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(name) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return "unknown"
}

type benchElectionResult struct {
	size  int
	stats benchStats
	stray int // nodes still alive after benchKillAll, summed over trials
}

// benchColdStart measures, per trial, the time from just before startCluster
// (NewNode arms each election timer) until the first node reports Leader.
func benchColdStart(size, trials int, out *benchWriter, progress func(string, ...any)) benchElectionResult {
	var samples []float64
	failures, stray := 0, 0
	for trial := range trials {
		start := time.Now()
		nodes := startCluster(size)
		leader, term, elapsed, ok := benchAwaitLeader(nodes, 0, start, benchElectionTimeout)
		stray += benchKillAll(nodes)

		if ok {
			samples = append(samples, benchMs(elapsed))
			out.add(size, trial, "ok", benchMs(elapsed), term, int(leader.ID()))
		} else {
			failures++
			out.add(size, trial, "timeout", benchMs(elapsed), "", "")
		}
		if (trial+1)%50 == 0 {
			progress("  cold start n=%d: %d/%d trials", size, trial+1, trials)
		}
	}
	return benchElectionResult{size: size, stats: benchSummarize(samples, failures), stray: stray}
}

// benchReelection measures, per trial, the time from Kill() on a settled
// leader until a surviving node is Leader in a higher term.
func benchReelection(size, trials int, out *benchWriter, progress func(string, ...any)) benchElectionResult {
	var samples []float64
	failures, stray := 0, 0
	for trial := range trials {
		nodes := startCluster(size)
		oldLeader, _, _, ok := benchAwaitLeader(nodes, 0, time.Now(), benchElectionTimeout)
		if !ok {
			// The initial election failing is also a failed trial.
			stray += benchKillAll(nodes)
			failures++
			out.add(size, trial, "timeout_initial_election", "", "", "", "", "")
			continue
		}
		time.Sleep(benchSettle)
		_, _, oldTerm, _, _ := oldLeader.Snapshot()

		start := time.Now()
		oldLeader.Kill()
		survivors := without(nodes, oldLeader)
		newLeader, newTerm, elapsed, ok := benchAwaitLeader(survivors, oldTerm, start, benchElectionTimeout)
		_, oldRole, _, _, _ := oldLeader.Snapshot()
		stray += benchKillAll(nodes)

		if ok {
			samples = append(samples, benchMs(elapsed))
			out.add(size, trial, "ok", benchMs(elapsed), oldTerm, newTerm, int(newLeader.ID()), oldRole)
		} else {
			failures++
			out.add(size, trial, "timeout", benchMs(elapsed), oldTerm, "", "", oldRole)
		}
		if (trial+1)%50 == 0 {
			progress("  re-election n=%d: %d/%d trials", size, trial+1, trials)
		}
	}
	return benchElectionResult{size: size, stats: benchSummarize(samples, failures), stray: stray}
}

type benchCommitResult struct {
	size        int
	stats       benchStats
	leaderLoss  int
	setupFailed string
}

// benchCommitLatency proposes commits entries one at a time on a stable
// cluster and measures Propose -> leader commitIndex >= returned index.
func benchCommitLatency(size, commits int, out *benchWriter, progress func(string, ...any)) benchCommitResult {
	res := benchCommitResult{size: size}
	nodes, leader, err := benchStableLeader(size)
	if err != nil {
		res.setupFailed = err.Error()
		res.stats = benchSummarize(nil, commits)
		return res
	}
	defer benchKillAll(nodes)

	var samples []float64
	failures := 0
	for seq := range commits {
		cmd := benchCommand(make([]byte, benchCommandSize), seq)
		start := time.Now()
		index, _, ok := leader.Propose(cmd)
		if !ok {
			failures++
			res.leaderLoss++
			out.add(size, seq, "not_leader", "", "")
			if leader, _, _, ok = benchAwaitLeader(nodes, 0, time.Now(), benchElectionTimeout); !ok {
				// No leader to propose to; every remaining commit fails.
				failures += commits - seq - 1
				break
			}
			continue
		}

		committed := false
		for {
			if _, _, _, commitIndex, _ := leader.Snapshot(); commitIndex >= index {
				committed = true
				break
			}
			if time.Since(start) > benchCommitTimeout {
				break
			}
			time.Sleep(benchCommitPoll)
		}
		elapsed := time.Since(start)
		if committed {
			samples = append(samples, benchMs(elapsed))
			out.add(size, seq, "ok", benchMs(elapsed), index)
		} else {
			failures++
			out.add(size, seq, "timeout", benchMs(elapsed), index)
		}
		if (seq+1)%500 == 0 {
			progress("  commit latency n=%d: %d/%d commits", size, seq+1, commits)
		}
	}
	res.stats = benchSummarize(samples, failures)
	return res
}

type benchThroughputRun struct {
	ok          bool
	err         string
	proposeMs   float64
	totalMs     float64
	commitsPerS float64
}

type benchThroughputResult struct {
	size   int
	runs   []benchThroughputRun
	median float64
}

// benchCheckLogs waits for every node to hold n entries and then checks that
// every log matches the leader's and the commands that were proposed.
func benchCheckLogs(nodes []*Node, leader *Node, cmds [][]byte) error {
	n := len(cmds)
	deadline := time.Now().Add(benchConvergeWait)
	for {
		done := true
		for _, node := range nodes {
			if _, _, _, _, logLen := node.Snapshot(); logLen != n {
				done = false
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			var lens []string
			for _, node := range nodes {
				_, _, _, _, logLen := node.Snapshot()
				lens = append(lens, fmt.Sprintf("node %d: %d", node.ID(), logLen))
			}
			return fmt.Errorf("logs did not converge to %d entries within %v (%s)", n, benchConvergeWait, strings.Join(lens, ", "))
		}
		time.Sleep(benchElectionPoll)
	}

	want := logCopy(leader)
	for i, entry := range want {
		if entry.Index != i || !bytes.Equal(entry.Command, cmds[i]) {
			return fmt.Errorf("leader log entry %d does not match the proposed command", i)
		}
	}
	for _, node := range nodes {
		got := logCopy(node)
		if len(got) != len(want) {
			return fmt.Errorf("node %d log length %d, leader has %d", node.ID(), len(got), len(want))
		}
		for i := range want {
			if got[i].Term != want[i].Term || got[i].Index != want[i].Index || !bytes.Equal(got[i].Command, want[i].Command) {
				return fmt.Errorf("node %d log entry %d (term %d) differs from leader (term %d)", node.ID(), i, got[i].Term, want[i].Term)
			}
		}
	}
	return nil
}

// benchThroughputOnce proposes n entries in a tight loop on a stable leader
// and measures first Propose -> leader commitIndex == n-1.
func benchThroughputOnce(size int, cmds [][]byte) benchThroughputRun {
	n := len(cmds)
	nodes, leader, err := benchStableLeader(size)
	if err != nil {
		return benchThroughputRun{err: err.Error()}
	}
	defer func() {
		benchKillAll(nodes)
		benchReleaseLogs(nodes)
	}()

	start := time.Now()
	for i, cmd := range cmds {
		index, _, ok := leader.Propose(cmd)
		if !ok {
			return benchThroughputRun{err: fmt.Sprintf("leader lost leadership at proposal %d", i)}
		}
		if index != i {
			return benchThroughputRun{err: fmt.Sprintf("proposal %d returned index %d", i, index)}
		}
	}
	proposeElapsed := time.Since(start)

	for {
		if _, _, _, commitIndex, _ := leader.Snapshot(); commitIndex >= n-1 {
			break
		}
		if time.Since(start) > benchThroughputWait {
			return benchThroughputRun{err: fmt.Sprintf("commitIndex did not reach %d within %v", n-1, benchThroughputWait)}
		}
		time.Sleep(benchElectionPoll)
	}
	total := time.Since(start)

	run := benchThroughputRun{
		ok:          true,
		proposeMs:   benchMs(proposeElapsed),
		totalMs:     benchMs(total),
		commitsPerS: float64(n) / total.Seconds(),
	}
	if err := benchCheckLogs(nodes, leader, cmds); err != nil {
		run.ok = false
		run.err = "LOG MISMATCH: " + err.Error()
	}
	return run
}

func benchThroughput(t *testing.T, size, n, runs int, out *benchWriter, progress func(string, ...any)) benchThroughputResult {
	res := benchThroughputResult{size: size}
	backing := make([]byte, n*benchCommandSize)
	cmds := make([][]byte, n)
	for i := range cmds {
		cmds[i] = benchCommand(backing[i*benchCommandSize:], i)
	}

	var good []float64
	for run := range runs {
		runtime.GC() // start each run from a clean heap, outside the timed region
		r := benchThroughputOnce(size, cmds)
		res.runs = append(res.runs, r)
		status := "ok"
		if !r.ok {
			status = r.err
			t.Errorf("throughput n=%d run %d FAILED: %s", size, run, r.err)
		} else {
			good = append(good, r.commitsPerS)
		}
		out.add(size, run, n, status, r.proposeMs, r.totalMs, r.commitsPerS)
		progress("  throughput n=%d run %d: %s, %.0f commits/sec (%.1f ms total, %.1f ms proposing)", size, run, status, r.commitsPerS, r.totalMs, r.proposeMs)
	}
	sort.Float64s(good)
	res.median = math.NaN()
	if len(good) > 0 {
		res.median = benchPercentile(good, 50)
	}
	return res
}

func benchStatsRow(label string, size int, s benchStats) string {
	return fmt.Sprintf("| %s | %d | %d | %d | %d | %s | %s | %s | %s | %s |",
		label, size, s.trials, s.ok, s.failures, benchFmt(s.min), benchFmt(s.p50), benchFmt(s.p95), benchFmt(s.p99), benchFmt(s.max))
}

func TestBench(t *testing.T) {
	if os.Getenv("RAFT_BENCH") != "1" {
		t.Skip("benchmark harness; set RAFT_BENCH=1 to run")
	}
	if benchRaceEnabled {
		t.Skip("benchmark harness must be run WITHOUT -race; the race detector distorts timings")
	}

	cfg := benchConfig{
		trials:  benchEnvInt(t, "RAFT_BENCH_TRIALS", 300),
		commits: benchEnvInt(t, "RAFT_BENCH_COMMITS", 2000),
		n:       benchEnvInt(t, "RAFT_BENCH_N", 200000),
		runs:    benchEnvInt(t, "RAFT_BENCH_RUNS", 5),
	}

	root := benchRepoRoot()
	resultsDir := filepath.Join(root, "bench-results")
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// raft.go prints a line per vote. Send stdout to /dev/null while trials
	// run and report progress on the original stdout.
	realStdout := os.Stdout
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer devNull.Close()
	progress := func(format string, args ...any) {
		fmt.Fprintf(realStdout, format+"\n", args...)
	}
	os.Stdout = devNull
	restored := false
	restore := func() {
		if !restored {
			os.Stdout = realStdout
			restored = true
		}
	}
	defer restore()

	started := time.Now()
	progress("RAFT benchmark: built WITHOUT -race. trials=%d commits=%d N=%d runs=%d", cfg.trials, cfg.commits, cfg.n, cfg.runs)

	coldOut := &benchWriter{header: []string{"cluster_size", "trial", "status", "ms", "term", "leader_id"}}
	reOut := &benchWriter{header: []string{"cluster_size", "trial", "status", "ms", "old_term", "new_term", "new_leader_id", "old_leader_role_at_detect"}}
	commitOut := &benchWriter{header: []string{"cluster_size", "seq", "status", "ms", "index"}}
	tputOut := &benchWriter{header: []string{"cluster_size", "run", "n", "status", "propose_ms", "total_ms", "commits_per_sec"}}

	var cold, re []benchElectionResult
	var commit []benchCommitResult
	var tput []benchThroughputResult
	for _, size := range benchClusterSizes {
		progress("cluster size %d", size)
		cold = append(cold, benchColdStart(size, cfg.trials, coldOut, progress))
		re = append(re, benchReelection(size, cfg.trials, reOut, progress))
		commit = append(commit, benchCommitLatency(size, cfg.commits, commitOut, progress))
		tput = append(tput, benchThroughput(t, size, cfg.n, cfg.runs, tputOut, progress))
	}
	wall := time.Since(started)
	restore()

	for name, w := range map[string]*benchWriter{
		"election_cold_start.csv": coldOut,
		"election_reelection.csv": reOut,
		"commit_latency.csv":      commitOut,
		"throughput.csv":          tputOut,
	} {
		if err := w.save(filepath.Join(resultsDir, name)); err != nil {
			t.Errorf("writing %s: %v", name, err)
		}
	}

	rerun := "RAFT_BENCH=1 go test -run TestBench -v -timeout 60m ./internal/raft"
	var overrides []string
	for _, name := range []string{"RAFT_BENCH_TRIALS", "RAFT_BENCH_COMMITS", "RAFT_BENCH_N", "RAFT_BENCH_RUNS"} {
		if v := os.Getenv(name); v != "" {
			overrides = append(overrides, name+"="+v)
		}
	}
	if len(overrides) > 0 {
		rerun = strings.Join(overrides, " ") + " " + rerun
	}

	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	w("# Raft benchmarks")
	w("")
	w("Generated by `internal/raft/bench_test.go`. Raw per-trial data is in `bench-results/`.")
	w("")
	w("**Network:** node-to-node RPC is in-memory (goroutines and Go channels via `NetworkRegistry`), not a real network. These numbers measure the algorithm and its timers in a single process, not network latency, serialization or disk I/O.")
	w("")
	w("**Build:** run WITHOUT `-race`.")
	w("")
	w("## Environment")
	w("")
	w("| | |")
	w("|---|---|")
	w("| Date | %s |", started.Format(time.RFC3339))
	w("| Git commit | %s |", benchGitCommit())
	w("| Go version | %s |", runtime.Version())
	w("| GOOS/GOARCH | %s/%s |", runtime.GOOS, runtime.GOARCH)
	w("| CPU | %s |", benchCPUModel())
	w("| NumCPU | %d |", runtime.NumCPU())
	w("| GOMAXPROCS | %d |", runtime.GOMAXPROCS(0))
	w("| Election trials per cluster size | %d (cold start) + %d (re-election) |", cfg.trials, cfg.trials)
	w("| Sequential commits per cluster size | %d |", cfg.commits)
	w("| Throughput | N = %d entries of %d bytes, %d runs per cluster size |", cfg.n, benchCommandSize, cfg.runs)
	w("| Wall time | %s |", wall.Round(time.Second))
	w("| Rerun | `%s` |", rerun)
	w("")
	w("## Leader election (ms)")
	w("")
	w("Cold start: fresh cluster per trial, measured from just before `startCluster` (which arms each node's election timer in `NewNode`) until the first node reports `Leader`. Re-election: fresh cluster, leader elected, %v of heartbeats, `Kill()` the leader, measured until a survivor is `Leader` in a higher term. Both polled every %v. Timeout %v; timed-out trials are counted as failures, and the percentiles cover successful trials only.", benchSettle, benchElectionPoll, benchElectionTimeout)
	w("")
	w("| Scenario | Nodes | Trials | OK | Failed | Min | p50 | p95 | p99 | Max |")
	w("|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range cold {
		w("%s", benchStatsRow("cold start", r.size, r.stats))
	}
	for _, r := range re {
		w("%s", benchStatsRow("re-election", r.size, r.stats))
	}
	w("")
	w("## Commit latency (ms)")
	w("")
	w("Stable cluster (leader elected, then %v of heartbeats). One %d-byte entry proposed to the leader, measured until the leader's `commitIndex` reaches the returned index, then the next one. Polled every %v. Timeout %v per commit.", benchSettle, benchCommandSize, benchCommitPoll, benchCommitTimeout)
	w("")
	w("| Scenario | Nodes | Commits | OK | Failed | Min | p50 | p95 | p99 | Max |")
	w("|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range commit {
		w("%s", benchStatsRow("sequential commit", r.size, r.stats))
	}
	for _, r := range commit {
		if r.setupFailed != "" {
			w("")
			w("**n=%d: setup failed:** %s", r.size, r.setupFailed)
		}
		if r.leaderLoss > 0 {
			w("")
			w("**n=%d:** leader lost leadership %d times during the run (counted as failures).", r.size, r.leaderLoss)
		}
	}
	w("")
	w("## Throughput (commits/sec)")
	w("")
	w("Stable cluster. N entries proposed in a tight loop on the leader, then wait until the leader's `commitIndex` is N-1. Throughput = N / (first Propose to final commit). After each run every node's log is checked for the same length, terms, indexes and commands.")
	w("")
	w("| Nodes | Run | Status | Propose loop (ms) | Total (ms) | Commits/sec |")
	w("|---|---|---|---|---|---|")
	for _, r := range tput {
		for i, run := range r.runs {
			status := "ok, logs match"
			if !run.ok {
				status = "FAILED: " + run.err
			}
			w("| %d | %d | %s | %s | %s | %.0f |", r.size, i, status, benchFmt(run.proposeMs), benchFmt(run.totalMs), run.commitsPerS)
		}
	}
	w("")
	w("| Nodes | Median commits/sec |")
	w("|---|---|")
	for _, r := range tput {
		median := "n/a (no successful runs)"
		if !math.IsNaN(r.median) {
			median = fmt.Sprintf("%.0f", r.median)
		}
		w("| %d | %s |", r.size, median)
	}

	var notes []string
	for _, rs := range [][]benchElectionResult{cold, re} {
		for _, r := range rs {
			if r.stray > 0 {
				notes = append(notes, fmt.Sprintf("n=%d: %d node(s) were still not Dead after repeated Kill() calls at the end of a trial (a fired election timer can revive a node after Kill)", r.size, r.stray))
			}
		}
	}
	if len(notes) > 0 {
		w("")
		w("## Notes")
		w("")
		for _, n := range notes {
			w("- %s", n)
		}
	}

	summary := b.String()
	if err := os.WriteFile(filepath.Join(root, "BENCHMARKS.md"), []byte(summary), 0o644); err != nil {
		t.Errorf("writing BENCHMARKS.md: %v", err)
	}
	fmt.Println()
	fmt.Print(summary)
}
