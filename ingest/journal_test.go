package ingest

import (
	"strings"
	"testing"
	"time"

	"github.com/openweft/weft-doctor/classify"
)

func TestParseJournalLine_KeepsPriorityLow(t *testing.T) {
	cases := []struct {
		name string
		prio string
		want bool
	}{
		{"emerg", "0", true},
		{"alert", "1", true},
		{"crit", "2", true},
		{"err", "3", true},
		{"warn", "4", false}, // warn without crash pattern → drop
		{"info", "6", false},
	}
	for _, tc := range cases {
		line := `{"MESSAGE":"routine boot ok","PRIORITY":"` + tc.prio + `","_SYSTEMD_UNIT":"weft-agent.service"}`
		_, ok := parseJournalLine(line, "dc1", "weft-agent.service")
		if ok != tc.want {
			t.Errorf("%s : got %v ; want %v", tc.name, ok, tc.want)
		}
	}
}

func TestParseJournalLine_MatchesCrashPattern(t *testing.T) {
	lines := []string{
		`{"MESSAGE":"panic: runtime error: nil pointer dereference","PRIORITY":"6"}`,
		`{"MESSAGE":"goroutine 47 [running]:","PRIORITY":"6"}`,
		`{"MESSAGE":"weft agent killed by signal: SIGKILL","PRIORITY":"6"}`,
		`{"MESSAGE":"Out of memory: Killed process 4321 (weft)","PRIORITY":"6"}`,
		`{"MESSAGE":"watchdog timed out after 30s","PRIORITY":"6"}`,
		`{"MESSAGE":"fatal error: concurrent map writes","PRIORITY":"6"}`,
	}
	for _, line := range lines {
		ev, ok := parseJournalLine(line, "dc1", "weft-agent.service")
		if !ok {
			t.Errorf("missed crash line : %s", line)
			continue
		}
		if ev.Source != "journal://dc1/weft-agent.service" {
			t.Errorf("unexpected source : %q", ev.Source)
		}
	}
}

func TestParseJournalLine_UnitResultFailure(t *testing.T) {
	line := `{"MESSAGE":"unit stopped","PRIORITY":"6","UNIT_RESULT":"signal","_SYSTEMD_UNIT":"weft-agent.service"}`
	ev, ok := parseJournalLine(line, "dc1", "weft-agent.service")
	if !ok {
		t.Fatal("UNIT_RESULT=signal should be kept")
	}
	if ev.Attrs["unit_result"] != "signal" {
		t.Errorf("unit_result attr missing : %v", ev.Attrs)
	}
}

func TestParseJournalLine_DropsRoutineLines(t *testing.T) {
	line := `{"MESSAGE":"weft-doctor running","PRIORITY":"6","UNIT_RESULT":"done"}`
	if _, ok := parseJournalLine(line, "dc1", "weft-agent.service"); ok {
		t.Error("routine info line should be dropped")
	}
}

func TestParseJournalLine_BadJSON(t *testing.T) {
	if _, ok := parseJournalLine("not json", "dc1", "weft-agent.service"); ok {
		t.Error("malformed JSON should be silently dropped")
	}
}

func TestParseRealtime(t *testing.T) {
	got := parseRealtime("1717939200000000") // 2024-06-09 12:00 UTC
	if got.Year() != 2024 || got.Month() != 6 {
		t.Errorf("realtime parse wrong : %v", got)
	}
	// Empty falls back to now ; just confirm it doesn't crash.
	if parseRealtime("").IsZero() {
		t.Error("empty realtime returned zero")
	}
}

func TestTruncate(t *testing.T) {
	s := strings.Repeat("a", 100)
	got := truncate(s, 50)
	if !strings.Contains(got, "truncated") {
		t.Errorf("truncate marker missing : %s", got)
	}
	if len(got) > 60 { // 50 + truncation marker overhead
		t.Errorf("truncate too long : %d", len(got))
	}
	if truncate("short", 100) != "short" {
		t.Error("truncate shouldn't touch short strings")
	}
}

// silentSink is a Sink that accumulates events for tests.
type silentSink struct {
	events []classify.LogEvent
}

func (s *silentSink) Add(ev classify.LogEvent) { s.events = append(s.events, ev) }

func TestNewJournalTailer_ValidationErrors(t *testing.T) {
	sink := &silentSink{}
	cases := []struct {
		name string
		opts JournalOptions
		want string
	}{
		{"no units", JournalOptions{Sink: sink}, "at least one unit"},
		{"no sink", JournalOptions{Units: []string{"x.service"}}, "sink is required"},
	}
	for _, tc := range cases {
		if _, err := NewJournalTailer(tc.opts); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s : got %v ; want %q", tc.name, err, tc.want)
		}
	}
}

func TestParseJournalLine_EventTimeIsParsed(t *testing.T) {
	// 1717944000_000000 microseconds since epoch = 2024-06-09 13:20:00 UTC.
	// We just verify the conversion round-trips exactly the same value
	// our parser computes — the absolute value doesn't matter beyond
	// being a known Linux microsecond timestamp.
	const us int64 = 1717944000_000000
	want := time.UnixMicro(us).UTC()
	line := `{"MESSAGE":"panic:","PRIORITY":"6","__REALTIME_TIMESTAMP":"1717944000000000"}`
	ev, ok := parseJournalLine(line, "dc1", "weft-agent.service")
	if !ok {
		t.Fatal("crash line dropped")
	}
	if !ev.Time.Equal(want) {
		t.Errorf("got %v ; want %v", ev.Time, want)
	}
}
