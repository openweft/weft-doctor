// Package llm abstracts the LLM call. V0.1 ships one impl (Ollama,
// local) ; the interface exists so V0.2 can add Anthropic / OpenAI
// / vLLM without touching the classify package.
package llm

import (
	"context"

	"github.com/openweft/weft-doctor/classify"
)

// Client classifies a batch of LogEvents into a set of Diagnoses.
// One LogEvent can be merged into another's Diagnosis via pattern_hash
// dedup ; the returned slice is the deduplicated set, never the input
// 1:1.
//
// Implementations MUST :
//   - respect ctx (the caller cancels on shutdown)
//   - return structured Diagnosis objects, never prose
//   - fill PatternHash deterministically so reruns dedup
//
// Caller pre-fills FirstSeen / LastSeen / Occurrences on the returned
// Diagnoses (the LLM doesn't see precise timestamps that survive
// quantization).
type Client interface {
	Classify(ctx context.Context, events []classify.LogEvent) ([]classify.Diagnosis, error)
}
