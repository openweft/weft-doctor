package buffer

import (
	"testing"
	"time"

	"github.com/openweft/weft-doctor/classify"
)

func TestBuffer_DedupSameSignature(t *testing.T) {
	b := New(Options{Window: time.Minute, BurstThreshold: 3, Now: fixedClock(0)})
	for i := 0; i < 5; i++ {
		b.Add(classify.LogEvent{Level: "ERROR", Msg: "VM start failed"})
	}
	// Even though we fed 5 events, the buffer holds ONE entry with
	// count=5 — the dedup key collapsed them.
	if len(b.events) != 1 {
		t.Errorf("want 1 entry, got %d", len(b.events))
	}
	for _, e := range b.events {
		if e.count != 5 {
			t.Errorf("count = %d ; want 5", e.count)
		}
	}
}

func TestBuffer_BurstTriggersBatch(t *testing.T) {
	b := New(Options{Window: time.Minute, BurstThreshold: 3, Now: fixedClock(0)})
	for i := 0; i < 5; i++ {
		b.Add(classify.LogEvent{Level: "ERROR", Msg: "VM start failed"})
	}
	batch := b.ReadyBatch(10)
	if len(batch) != 1 {
		t.Errorf("ReadyBatch returned %d ; want 1 (burst over threshold)", len(batch))
	}
}

func TestBuffer_BelowThresholdNotReady(t *testing.T) {
	b := New(Options{Window: time.Minute, BurstThreshold: 5, Now: fixedClock(0)})
	for i := 0; i < 3; i++ {
		b.Add(classify.LogEvent{Level: "ERROR", Msg: "VM start failed"})
	}
	batch := b.ReadyBatch(10)
	if len(batch) != 0 {
		t.Errorf("ReadyBatch returned %d ; want 0 (under threshold)", len(batch))
	}
}

func TestBuffer_EmittedSuppression(t *testing.T) {
	// After ReadyBatch emits a signature, subsequent Add for the
	// same signature should NOT make it ready again within the
	// same window. Avoids flapping when the burst continues.
	b := New(Options{Window: time.Minute, BurstThreshold: 2, Now: fixedClock(0)})
	for i := 0; i < 5; i++ {
		b.Add(classify.LogEvent{Level: "ERROR", Msg: "VM start failed"})
	}
	first := b.ReadyBatch(10)
	if len(first) != 1 {
		t.Fatalf("first batch = %d ; want 1", len(first))
	}
	// More events come in for the same signature.
	for i := 0; i < 5; i++ {
		b.Add(classify.LogEvent{Level: "ERROR", Msg: "VM start failed"})
	}
	second := b.ReadyBatch(10)
	if len(second) != 0 {
		t.Errorf("second batch = %d ; want 0 (suppressed)", len(second))
	}
}

func TestBuffer_PruneOldEntries(t *testing.T) {
	now := int64(1000)
	b := New(Options{Window: 60 * time.Second, BurstThreshold: 1, Now: func() time.Time { return time.Unix(now, 0) }})

	b.Add(classify.LogEvent{Level: "ERROR", Msg: "old"})
	now += 120 // advance 2 minutes — past window
	b.Add(classify.LogEvent{Level: "ERROR", Msg: "fresh"})

	// "old" should be pruned, only "fresh" remains.
	if len(b.events) != 1 {
		t.Errorf("want 1 entry after prune, got %d", len(b.events))
	}
	for _, e := range b.events {
		if e.sampleData.Msg != "fresh" {
			t.Errorf("kept wrong entry : %v", e.sampleData)
		}
	}
}

func TestBuffer_MaxBatchCap(t *testing.T) {
	// 5 distinct signatures all burst, but maxBatch=3 → top 3 win.
	b := New(Options{Window: time.Minute, BurstThreshold: 2, Now: fixedClock(0)})
	msgs := []string{"a", "b", "c", "d", "e"}
	counts := []int{5, 4, 3, 2, 1}
	for i, m := range msgs {
		for j := 0; j < counts[i]; j++ {
			b.Add(classify.LogEvent{Level: "ERROR", Msg: m})
		}
	}
	batch := b.ReadyBatch(3)
	if len(batch) != 3 {
		t.Fatalf("batch = %d ; want 3", len(batch))
	}
	// Top 3 by count are a (5), b (4), c (3). Verify they're present
	// and the lowest-count signatures didn't sneak in.
	gotMsgs := map[string]bool{}
	for _, e := range batch {
		gotMsgs[e.Msg] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !gotMsgs[want] {
			t.Errorf("missing %q in batch", want)
		}
	}
	for _, denied := range []string{"d", "e"} {
		if gotMsgs[denied] {
			t.Errorf("low-count %q should not be in batch", denied)
		}
	}
}

func TestSignature_DeterministicAcrossInvocations(t *testing.T) {
	e1 := classify.LogEvent{Level: "ERROR", Msg: "VM start failed"}
	e2 := classify.LogEvent{Level: "ERROR", Msg: "VM start failed"}
	if Signature(e1) != Signature(e2) {
		t.Error("same level+msg should produce same signature")
	}
}

func TestSignature_AttributesNotInKey(t *testing.T) {
	// Two events with same Msg but different Attrs should collapse —
	// the operator wants one Diagnosis for the pattern, not one per VM.
	e1 := classify.LogEvent{Level: "ERROR", Msg: "VM start failed", Attrs: map[string]any{"vm_uuid": "abc"}}
	e2 := classify.LogEvent{Level: "ERROR", Msg: "VM start failed", Attrs: map[string]any{"vm_uuid": "xyz"}}
	if Signature(e1) != Signature(e2) {
		t.Error("attrs should not affect signature")
	}
}

func fixedClock(unix int64) func() time.Time {
	return func() time.Time { return time.Unix(unix, 0) }
}
