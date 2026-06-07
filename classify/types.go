// Package classify owns the structured types that flow through the
// weft-doctor pipeline : the raw LogEvent ingested from NATS / slog,
// and the Diagnosis emitted by the LLM after classification. Types
// here are CONTRACT — every other package consumes them by value, so
// changing fields here ripples through the whole binary.
package classify

import "time"

// LogEvent is one parsed slog JSON record from a weft component.
// Matches the structure slog.JSONHandler emits, with the openweft
// convention that every weft component logs to stderr in JSON.
type LogEvent struct {
	// Time is when the log line was emitted by the source component.
	Time time.Time `json:"time"`
	// Level is slog's level string ("DEBUG", "INFO", "WARN", "ERROR").
	// weft-doctor only buffers WARN+ERROR (filtered at ingest).
	Level string `json:"level"`
	// Msg is the human-readable message.
	Msg string `json:"msg"`
	// Attrs are the structured key-value pairs slog attached.
	// Common ones : err, vm_uuid, project, peer, lsn, retry.
	Attrs map[string]any `json:"attrs,omitempty"`
	// Source is the NATS subject we received this event on (e.g.
	// "weft.agent.dc1-r1-h1.event"). Empty when the event came from
	// a non-NATS ingest (file tail, gRPC stream).
	Source string `json:"source,omitempty"`
}

// Severity is the diagnosis ranking. Renders as a colored badge in
// the GitHub dashboard issue ; drives ordering in NATS output.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityHigh     Severity = "high"
	SeverityMedium   Severity = "medium"
	SeverityLow      Severity = "low"
)

// Diagnosis is the structured LLM output for one detected pattern.
// One Diagnosis covers ONE underlying root cause even if it was
// observed N times — Occurrences captures the count, Examples carries
// up to 3 representative events.
type Diagnosis struct {
	// PatternHash is a deterministic identifier for the pattern. The
	// LLM is prompted to produce a stable hash for the (signature,
	// root_cause) pair so subsequent runs can dedup against an
	// existing dashboard entry rather than appending a new one.
	PatternHash string `json:"pattern_hash"`
	// Severity is the LLM's ranking of how urgently this needs the
	// operator's attention.
	Severity Severity `json:"severity"`
	// Title is a one-line human-readable summary, used as the
	// dashboard entry heading.
	Title string `json:"title"`
	// RootCause is the LLM's hypothesis on what's wrong.
	RootCause string `json:"root_cause"`
	// SuggestedAction is what the operator should DO next ; the
	// single most-useful command or change. Aim for one line.
	SuggestedAction string `json:"suggested_action"`
	// FileLocation, when non-empty, points at the source file:line
	// the LLM thinks the bug lives in. Inferred from stack traces
	// in the buffered events. Null is fine when there's no stack.
	FileLocation string `json:"file_location,omitempty"`
	// Occurrences is how many raw events match this pattern in the
	// buffer window. Drives "10x in 5min" style burst metrics.
	Occurrences int `json:"occurrences"`
	// FirstSeen / LastSeen bound the time window the pattern was
	// observed in. Both are filled by the buffer before invoking the
	// LLM ; the LLM doesn't re-derive them.
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	// Examples carries up to 3 representative LogEvents so the
	// operator can see the raw evidence without leaving the
	// dashboard. The LLM picks them ; if it can't, the buffer fills
	// the first N.
	Examples []LogEvent `json:"examples,omitempty"`
}
