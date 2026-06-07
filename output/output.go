// Package output dispatches finalised Diagnoses to their sinks. V0.1
// ships two : a NATS publisher (for downstream consumers — webui,
// alerting, runners) and a GitHub Dashboard-issue maintainer (one
// long-lived issue per target repo, updated in place).
package output

import (
	"context"

	"github.com/openweft/weft-doctor/classify"
)

// Sink delivers a batch of Diagnoses. Implementations MAY be
// best-effort (a transient failure should not abort the dispatch
// loop) ; the wrapper Multi handles fan-out.
type Sink interface {
	// Name identifies the sink for logging ("nats", "github").
	Name() string
	// Publish delivers the diagnoses. Returns the first error
	// encountered ; implementations should NOT panic.
	Publish(ctx context.Context, diags []classify.Diagnosis) error
}

// Multi fans out one batch to several sinks. A failing sink's error
// is logged by the caller ; Multi never aborts the whole fan-out.
type Multi struct {
	Sinks []Sink
}

// Publish writes to each sink in order. Errors are collected and
// joined ; if every sink succeeds the return is nil.
func (m *Multi) Publish(ctx context.Context, diags []classify.Diagnosis) []error {
	var errs []error
	for _, s := range m.Sinks {
		if err := s.Publish(ctx, diags); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
