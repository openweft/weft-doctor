package output

import (
	"strings"
	"testing"
	"time"

	"github.com/openweft/weft-doctor/classify"
)

// TestRenderBody_Empty : no diagnoses → "all quiet" placeholder.
func TestRenderBody_Empty(t *testing.T) {
	s := &GitHubSink{state: map[string]classify.Diagnosis{}, maxRecent: 20}
	body := s.renderBody()
	if !strings.Contains(body, "All quiet") {
		t.Errorf("empty body missing 'All quiet' marker : %s", body)
	}
}

// TestRenderBody_SortedBySeverity : critical first, then high, etc.
// Within same severity, higher occurrences first.
func TestRenderBody_SortedBySeverity(t *testing.T) {
	s := &GitHubSink{
		state: map[string]classify.Diagnosis{
			"a": {PatternHash: "a", Severity: classify.SeverityLow, Title: "low alpha", Occurrences: 5},
			"b": {PatternHash: "b", Severity: classify.SeverityCritical, Title: "crit bravo", Occurrences: 1},
			"c": {PatternHash: "c", Severity: classify.SeverityHigh, Title: "high charlie", Occurrences: 100},
			"d": {PatternHash: "d", Severity: classify.SeverityCritical, Title: "crit delta", Occurrences: 50},
		},
		maxRecent: 20,
	}
	body := s.renderBody()
	idxBravo := strings.Index(body, "crit bravo")
	idxDelta := strings.Index(body, "crit delta")
	idxCharlie := strings.Index(body, "high charlie")
	idxAlpha := strings.Index(body, "low alpha")
	// Both criticals must come before high+low.
	for name, idx := range map[string]int{"bravo": idxBravo, "delta": idxDelta} {
		if idx == -1 {
			t.Fatalf("missing %s in body", name)
		}
		if idx > idxCharlie || idx > idxAlpha {
			t.Errorf("critical %s should sort before high/low", name)
		}
	}
	// Within critical, delta (50) > bravo (1).
	if idxDelta > idxBravo {
		t.Errorf("higher-occurrence critical (delta, 50) should come before lower (bravo, 1)")
	}
}

// TestRenderBody_MaxRecentCap : when we have more diagnoses than the
// cap, only the top N (by sort order) survive.
func TestRenderBody_MaxRecentCap(t *testing.T) {
	state := map[string]classify.Diagnosis{}
	for i := 0; i < 30; i++ {
		state[string(rune('a'+i))] = classify.Diagnosis{
			PatternHash: string(rune('a' + i)),
			Severity:    classify.SeverityLow,
			Title:       "diag-" + string(rune('a'+i)),
			Occurrences: i,
		}
	}
	s := &GitHubSink{state: state, maxRecent: 5}
	body := s.renderBody()
	// Should mention "5 active" or have 5 entries — count "## " section
	// headings (which our writeEntry emits).
	got := strings.Count(body, "## 🟢")
	if got != 5 {
		t.Errorf("got %d entries in body ; want 5 (maxRecent cap)", got)
	}
}

// TestRenderBody_IncludesEverything : a fully-populated diagnosis
// must surface every field in the markdown (operator wants the
// full forensic picture).
func TestRenderBody_IncludesEverything(t *testing.T) {
	now := time.Date(2026, 6, 7, 20, 0, 0, 0, time.UTC)
	d := classify.Diagnosis{
		PatternHash:     "abc123",
		Severity:        classify.SeverityCritical,
		Title:           "primary postgres lost on dc2",
		RootCause:       "disk full on /var/lib/pgsql triggers fatal exit",
		SuggestedAction: "df -h on dc2-r1-h1, free up space, then `rcctl restart postgres`",
		FileLocation:    "internal/postgres/controller.go:142",
		Occurrences:     17,
		FirstSeen:       now.Add(-10 * time.Minute),
		LastSeen:        now,
		Examples: []classify.LogEvent{
			{Level: "ERROR", Msg: "FATAL: disk full", Source: "weft.agent.dc2-r1-h1"},
		},
	}
	s := &GitHubSink{state: map[string]classify.Diagnosis{"abc123": d}, maxRecent: 20}
	body := s.renderBody()
	wants := []string{
		"primary postgres lost on dc2",
		"disk full on /var/lib/pgsql",
		"rcctl restart postgres",
		"internal/postgres/controller.go:142",
		"abc123",
		"17",
		"FATAL: disk full",
		"weft.agent.dc2-r1-h1",
	}
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("body missing %q", w)
		}
	}
}
