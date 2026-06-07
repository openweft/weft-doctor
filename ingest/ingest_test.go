package ingest

import (
	"reflect"
	"testing"

	"github.com/openweft/weft-doctor/classify"
)

// TestParseSlog_ValidRecord : a textbook slog.JSONHandler line
// decodes cleanly into a LogEvent with attrs split from top-level.
func TestParseSlog_ValidRecord(t *testing.T) {
	raw := []byte(`{"time":"2026-06-07T20:00:00Z","level":"ERROR","msg":"VM start failed","vm_uuid":"abc","retry":3}`)
	ev, ok := parseSlog(raw)
	if !ok {
		t.Fatal("parseSlog returned not-ok for valid record")
	}
	if ev.Level != "ERROR" || ev.Msg != "VM start failed" {
		t.Errorf("level/msg = %q/%q", ev.Level, ev.Msg)
	}
	want := map[string]any{"vm_uuid": "abc", "retry": float64(3)}
	if !reflect.DeepEqual(ev.Attrs, want) {
		t.Errorf("Attrs = %v ; want %v", ev.Attrs, want)
	}
}

// TestParseSlog_MissingLevel : a record without level is rejected
// (we can't classify it).
func TestParseSlog_MissingLevel(t *testing.T) {
	raw := []byte(`{"time":"2026-06-07T20:00:00Z","msg":"no level here"}`)
	_, ok := parseSlog(raw)
	if ok {
		t.Error("parseSlog should reject record without level")
	}
}

// TestParseSlog_BinaryGarbage : non-JSON data is silently rejected
// (we don't want to log on every status ping in the cluster).
func TestParseSlog_BinaryGarbage(t *testing.T) {
	raw := []byte("not json at all")
	_, ok := parseSlog(raw)
	if ok {
		t.Error("parseSlog should reject non-JSON")
	}
}

// TestParseSlog_NoAttrs : top-level only (time/level/msg) yields
// nil Attrs, not an empty map. Keeps downstream allocations lean.
func TestParseSlog_NoAttrs(t *testing.T) {
	raw := []byte(`{"time":"2026-06-07T20:00:00Z","level":"WARN","msg":"only"}`)
	ev, ok := parseSlog(raw)
	if !ok {
		t.Fatal("expected ok")
	}
	if ev.Attrs != nil {
		t.Errorf("Attrs should be nil for no-extras record ; got %v", ev.Attrs)
	}
}

// fakeSink captures Add calls for assertion in higher-level tests.
type fakeSink struct {
	got []classify.LogEvent
}

func (s *fakeSink) Add(e classify.LogEvent) {
	s.got = append(s.got, e)
}

// Verify the type at compile time.
var _ Sink = (*fakeSink)(nil)
