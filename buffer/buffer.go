// Package buffer holds incoming LogEvents in a sliding window, dedup
// by signature, and surfaces "ready for diagnosis" batches when a
// burst threshold is crossed.
//
// The buffer is the rate-limiter between ingest and the LLM : without
// it, a single retry loop in some weft component would spam Ollama
// with hundreds of identical events per minute. With it, those events
// fold into one batch with a high Occurrences count.
package buffer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/openweft/weft-doctor/classify"
)

// Options configures a Buffer.
type Options struct {
	// Window is how far back the buffer keeps events. Older entries
	// are pruned. Default 5 minutes.
	Window time.Duration
	// BurstThreshold is the per-signature occurrence count that
	// triggers a "ready for diagnosis" signal. Default 10.
	BurstThreshold int
	// Now is a clock seam for tests. Default time.Now.
	Now func() time.Time
}

// Buffer is a thread-safe sliding-window event store with dedup.
//
// Concurrency model : Add can be called from many ingest goroutines.
// ReadyBatch is called from a single dispatcher goroutine on a
// schedule (e.g. every 10s). The internal mutex serialises the two.
type Buffer struct {
	window    time.Duration
	threshold int
	now       func() time.Time

	mu       sync.Mutex
	events   map[string]*entry // signature → entry
	emitted  map[string]bool   // signature already emitted in current window
	emitTime map[string]time.Time
}

// entry tracks one signature's occurrences in the window.
type entry struct {
	first      time.Time
	last       time.Time
	count      int
	signature  string
	sampleData classify.LogEvent
}

// New builds a Buffer with defaults filled in.
func New(opts Options) *Buffer {
	if opts.Window == 0 {
		opts.Window = 5 * time.Minute
	}
	if opts.BurstThreshold == 0 {
		opts.BurstThreshold = 10
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Buffer{
		window:    opts.Window,
		threshold: opts.BurstThreshold,
		now:       opts.Now,
		events:    map[string]*entry{},
		emitted:   map[string]bool{},
		emitTime:  map[string]time.Time{},
	}
}

// Add records one LogEvent. Cheap path : O(1) map lookup + counter
// increment when the signature is already present, which is the
// common case during a burst.
func (b *Buffer) Add(e classify.LogEvent) {
	sig := Signature(e)
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(now)
	if existing, ok := b.events[sig]; ok {
		existing.count++
		existing.last = now
		return
	}
	b.events[sig] = &entry{
		first:      now,
		last:       now,
		count:      1,
		signature:  sig,
		sampleData: e,
	}
}

// ReadyBatch returns up to maxBatch signatures whose count exceeds
// the burst threshold AND that we haven't already emitted in the
// current window. After return, those signatures are marked emitted ;
// re-burst within the same window is suppressed (prevents flapping).
//
// The returned slice is sorted by Occurrences descending so the LLM
// sees the loudest signal first ; if the model's context truncates,
// the dropped signals are the quieter ones.
func (b *Buffer) ReadyBatch(maxBatch int) []classify.LogEvent {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(now)
	type ready struct {
		sig   string
		count int
		ev    classify.LogEvent
	}
	pool := make([]ready, 0)
	for sig, e := range b.events {
		if e.count < b.threshold {
			continue
		}
		if b.emitted[sig] {
			continue
		}
		pool = append(pool, ready{sig: sig, count: e.count, ev: e.sampleData})
	}
	sort.Slice(pool, func(i, j int) bool { return pool[i].count > pool[j].count })
	if maxBatch > 0 && len(pool) > maxBatch {
		pool = pool[:maxBatch]
	}
	out := make([]classify.LogEvent, len(pool))
	for i, r := range pool {
		out[i] = r.ev
		b.emitted[r.sig] = true
		b.emitTime[r.sig] = now
	}
	return out
}

// Lookup returns the (FirstSeen, LastSeen, Occurrences) tuple for a
// signature. Used by the dispatch loop to enrich the LLM's Diagnosis
// output with precise timing/count info that the LLM doesn't see.
func (b *Buffer) Lookup(sig string) (first, last time.Time, count int, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.events[sig]
	if !ok {
		return time.Time{}, time.Time{}, 0, false
	}
	return e.first, e.last, e.count, true
}

// prune drops entries older than the window AND clears the emitted-
// suppression for signatures that have aged out. Called under lock.
func (b *Buffer) prune(now time.Time) {
	cutoff := now.Add(-b.window)
	for sig, e := range b.events {
		if e.last.Before(cutoff) {
			delete(b.events, sig)
		}
	}
	for sig, when := range b.emitTime {
		if when.Before(cutoff) {
			delete(b.emitted, sig)
			delete(b.emitTime, sig)
		}
	}
}

// Signature is the dedup key for one LogEvent. We normalise on
// (Level, Msg) which is right for the dominant case of "same error
// fires N times". Per-event attributes (vm_uuid, retry count, etc.)
// are deliberately NOT in the signature : two events with the same
// error message on different VMs should fold together so the LLM
// sees "VM lifecycle errors x47" rather than 47 separate diagnoses.
//
// Exported because the LLM may suggest a refined pattern_hash that
// the dispatcher can correlate back to this signature via Lookup.
func Signature(e classify.LogEvent) string {
	// SHA-256 truncated to 12 hex chars — collision-resistant within
	// any plausible openweft event volume.
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s", e.Level, e.Msg)))
	return hex.EncodeToString(h[:6])
}
