package output

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/openweft/weft-doctor/classify"
)

// NATSSink publishes one Diagnosis per message to a configurable
// subject prefix. Subject layout :
//
//	<prefix>.<severity>.<pattern_hash>
//
// e.g. weft.diagnosis.critical.a7f3b201 — downstream consumers can
// filter on severity wildcards (`weft.diagnosis.critical.>`) or
// catch the lot (`weft.diagnosis.>`).
type NATSSink struct {
	conn   *nats.Conn
	prefix string
}

// NewNATSSink takes an existing NATS connection so the operator
// reuses the same connection the ingester opened ; one connection
// per process is the recommended NATS pattern.
//
// prefix defaults to "weft.diagnosis" when empty.
func NewNATSSink(conn *nats.Conn, prefix string) *NATSSink {
	if prefix == "" {
		prefix = "weft.diagnosis"
	}
	return &NATSSink{conn: conn, prefix: prefix}
}

func (s *NATSSink) Name() string { return "nats" }

func (s *NATSSink) Publish(_ context.Context, diags []classify.Diagnosis) error {
	for _, d := range diags {
		body, err := json.Marshal(d)
		if err != nil {
			return fmt.Errorf("marshal diagnosis %s: %w", d.PatternHash, err)
		}
		subj := fmt.Sprintf("%s.%s.%s", s.prefix, d.Severity, d.PatternHash)
		if err := s.conn.Publish(subj, body); err != nil {
			return fmt.Errorf("publish %s: %w", subj, err)
		}
	}
	return s.conn.Flush()
}
