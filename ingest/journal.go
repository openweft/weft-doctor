package ingest

// journal.go is the V0.2 systemd-journal tailer. It runs
// `journalctl --output=json -f -u <unit>` per configured systemd
// unit, parses each JSON line, filters crash-relevant entries (Go
// panic stack traces, SIGKILL, OOM-kill, segfault, fatal errors),
// and feeds them into the same Sink the NATSIngester uses.
//
// Closes the V0.1 gap : the existing NATS subscriber only carries
// slog records, so a panic / SIGKILL / OOM that takes a component
// down before slog can fan out goes silent. With this tailer the
// kernel + systemd records land in the buffer and weft-doctor's
// LLM classifier sees them.
//
// One JournalTailer per host ; multiple units in a single tailer
// (one journalctl child process per unit so each tail is
// independent).

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/openweft/weft-doctor/classify"
)

// JournalOptions configures one tailer.
type JournalOptions struct {
	// Units are the systemd unit names to tail. Empty disables the
	// tailer (the main routine should skip starting it).
	Units []string
	// Host is the host identifier surfaced on every LogEvent. Empty
	// = use os.Hostname() at start time.
	Host string
	// Sink receives parsed events. Same Sink the NATSIngester feeds.
	Sink Sink
	// Logger : structured progress.
	Logger *slog.Logger
	// JournalctlBinary lets tests stub out the real binary. Empty
	// resolves to PATH "journalctl".
	JournalctlBinary string
}

// JournalTailer fans out one journalctl child per unit, parses
// each emitted JSON line, and feeds the matching ones into Sink.
type JournalTailer struct {
	opts JournalOptions
	log  *slog.Logger

	cmds   []*exec.Cmd
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewJournalTailer validates options + returns a ready-to-Run
// tailer. The tailer does NOT spawn anything until Run is called.
func NewJournalTailer(opts JournalOptions) (*JournalTailer, error) {
	if len(opts.Units) == 0 {
		return nil, errors.New("journal: at least one unit is required")
	}
	if opts.Sink == nil {
		return nil, errors.New("journal: sink is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.JournalctlBinary == "" {
		opts.JournalctlBinary = "journalctl"
	}
	return &JournalTailer{opts: opts, log: opts.Logger}, nil
}

// Run starts one journalctl child per unit + reads each child's
// stdout in parallel. Blocks until ctx is cancelled, then waits for
// every child to exit before returning.
func (t *JournalTailer) Run(ctx context.Context) error {
	rctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	for _, unit := range t.opts.Units {
		t.wg.Add(1)
		go t.tailOne(rctx, unit)
	}
	<-rctx.Done()
	t.wg.Wait()
	return ctx.Err()
}

// Close stops the tailer + waits for every child to drain. Safe to
// call multiple times.
func (t *JournalTailer) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	return nil
}

// tailOne spawns one journalctl child for a unit and pumps its
// stdout through parseJournalLine into the Sink. On child exit
// (signal, host shutdown, journalctl crash) the loop retries with
// a 5-second backoff so the tailer survives the kind of transient
// failures journalctl itself produces.
func (t *JournalTailer) tailOne(ctx context.Context, unit string) {
	defer t.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := t.runJournalctl(ctx, unit); err != nil && ctx.Err() == nil {
			t.log.Warn("journal tailer : child exited, retrying", "unit", unit, "err", err)
		}
		// Backoff between child restarts. Short enough to recover
		// fast, long enough to avoid hot-looping on a bad command.
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// runJournalctl is one full child lifecycle : spawn, read, wait.
func (t *JournalTailer) runJournalctl(ctx context.Context, unit string) error {
	args := []string{
		"--output=json",
		"--follow",
		"--no-pager",
		"--unit=" + unit,
		"--since=now", // ignore historical events on tailer start
	}
	cmd := exec.CommandContext(ctx, t.opts.JournalctlBinary, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	t.log.Info("journal tailer : following unit", "unit", unit, "pid", cmd.Process.Pid)

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1<<20) // 1MiB max line — covers a full Go stack trace
	for scanner.Scan() {
		line := scanner.Text()
		ev, ok := parseJournalLine(line, t.opts.Host, unit)
		if !ok {
			continue
		}
		t.opts.Sink.Add(ev)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("scan: %w", err)
	}
	return cmd.Wait()
}

// journalRecord is the subset of `journalctl --output=json` we read.
// The actual schema has dozens of fields ; only these matter for
// crash classification.
type journalRecord struct {
	Realtime          string `json:"__REALTIME_TIMESTAMP"` // microseconds since epoch
	Message           string `json:"MESSAGE"`
	Priority          string `json:"PRIORITY"`
	Unit              string `json:"_SYSTEMD_UNIT"`
	Pid               string `json:"_PID"`
	BootID            string `json:"_BOOT_ID"`
	Comm              string `json:"_COMM"`
	ExitStatus        string `json:"EXIT_STATUS"` // present on systemd Type=notify exit lines
	JobResult         string `json:"JOB_RESULT"`
	UnitResult        string `json:"UNIT_RESULT"`
	NewMain           string `json:"NEW_MAIN_PID"`
	OOMScore          string `json:"COREDUMP_TIMESTAMP"`
}

// crashPatterns identify lines worth forwarding to the LLM. Tested
// against the MESSAGE field. Conservative on purpose : too many
// matches would flood the buffer ; better to miss a soft warning
// than to drown the classifier.
var crashPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bpanic:`),
	regexp.MustCompile(`(?i)\bgoroutine \d+ \[running\]:`),
	regexp.MustCompile(`(?i)\bsegmentation fault\b`),
	regexp.MustCompile(`(?i)\bsigsegv\b`),
	regexp.MustCompile(`(?i)\bsigabrt\b`),
	regexp.MustCompile(`(?i)\bsigkill\b`),
	regexp.MustCompile(`(?i)\bsigbus\b`),
	regexp.MustCompile(`(?i)\boom[ -]killer\b`),
	regexp.MustCompile(`(?i)\bout of memory\b`),
	regexp.MustCompile(`(?i)\bkilled by signal\b`),
	regexp.MustCompile(`(?i)\bwatchdog timed out\b`),
	regexp.MustCompile(`(?i)\bfatal error:`),
	regexp.MustCompile(`(?i)\bunrecoverable\b`),
}

// parseJournalLine decodes one journalctl JSON line and decides
// whether it's worth feeding to the classifier. Returns (event,
// true) on match, (_, false) on parse error or non-crash line.
//
// Coverage decisions :
//   - PRIORITY 0-3 (emerg/alert/crit/err) → always forwarded.
//   - PRIORITY ≥4 but MESSAGE matches a crash pattern → forwarded.
//   - JOB_RESULT/UNIT_RESULT != "done"/"" → forwarded (systemd
//     surfaces unit failures via these fields).
//   - Everything else → dropped.
func parseJournalLine(line, host, unit string) (classify.LogEvent, bool) {
	if line == "" {
		return classify.LogEvent{}, false
	}
	var r journalRecord
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		return classify.LogEvent{}, false
	}

	prio := parsePriority(r.Priority)
	matched := matchesCrashPattern(r.Message)
	failureResult := r.UnitResult != "" && r.UnitResult != "done"
	jobFailure := r.JobResult != "" && r.JobResult != "done"

	if prio > 3 && !matched && !failureResult && !jobFailure {
		return classify.LogEvent{}, false
	}

	ev := classify.LogEvent{
		Time:   parseRealtime(r.Realtime),
		Level:  prioToLevel(prio),
		Msg:    truncate(r.Message, 8192),
		Source: "journal://" + host + "/" + unit,
		Attrs: map[string]any{
			"unit":         unit,
			"host":         host,
			"pid":          r.Pid,
			"comm":         r.Comm,
			"priority":     prio,
			"exit_status":  r.ExitStatus,
			"job_result":   r.JobResult,
			"unit_result":  r.UnitResult,
			"new_main_pid": r.NewMain,
			"boot_id":      r.BootID,
		},
	}
	return ev, true
}

func parsePriority(s string) int {
	switch s {
	case "0":
		return 0
	case "1":
		return 1
	case "2":
		return 2
	case "3":
		return 3
	case "4":
		return 4
	case "5":
		return 5
	case "6":
		return 6
	case "7":
		return 7
	default:
		// Missing field = treat as info (6) so the message-pattern
		// + result-field paths still get evaluated.
		return 6
	}
}

func prioToLevel(prio int) string {
	switch {
	case prio <= 3:
		return "ERROR"
	case prio == 4:
		return "WARN"
	default:
		return "INFO"
	}
}

func parseRealtime(s string) time.Time {
	if s == "" {
		return time.Now().UTC()
	}
	// systemd journal microseconds since unix epoch.
	var us int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return time.Now().UTC()
		}
		us = us*10 + int64(c-'0')
	}
	return time.UnixMicro(us).UTC()
}

func matchesCrashPattern(msg string) bool {
	if msg == "" {
		return false
	}
	for _, re := range crashPatterns {
		if re.MatchString(msg) {
			return true
		}
	}
	return false
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	// Keep the head (where the panic header lives) and the tail
	// (where the runtime stack lands) — drop the middle.
	head := max / 2
	tail := max - head - 20
	return s[:head] + "\n... [truncated] ...\n" + s[len(s)-tail:]
}

// strings ref so the import isn't flagged unused if we drop a use.
var _ = strings.TrimSpace
