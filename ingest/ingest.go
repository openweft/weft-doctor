// Package ingest connects weft-doctor to the event sources. V0.1
// supports one : a NATS subscriber that fans in JSON-encoded slog
// events from every subject the operator wires.
//
// Per the openweft pattern (every component logs to stderr as JSON
// slog), weft-agent + microvm-agent + drivers already publish their
// stderr lines to NATS subjects via a small slog Handler that sits
// between the component and stderr. weft-doctor subscribes ; it does
// NOT tail files or read from systemd journals (out of scope for V0.1).
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go"

	"github.com/openweft/weft-doctor/classify"
)

// Sink is the destination for parsed LogEvents — typically a
// buffer.Buffer. The interface exists so tests can replace it with
// a slice-accumulator.
type Sink interface {
	Add(classify.LogEvent)
}

// NATSIngester subscribes to one or more NATS subjects, parses each
// message as a JSON slog record, filters to WARN+ERROR, and feeds
// the Sink.
type NATSIngester struct {
	conn     *nats.Conn
	subjects []string
	sink     Sink
	log      *slog.Logger

	subs []*nats.Subscription
}

// NATSOptions configures the ingester.
type NATSOptions struct {
	// URL is the NATS endpoint. Required.
	URL string
	// Subjects are the wildcards to subscribe to. Use "weft.>" to
	// catch the whole namespace ; finer slices keep the buffer
	// focused.
	Subjects []string
	// Sink receives parsed events. Typically *buffer.Buffer.
	Sink Sink
	// Logger is the slog logger.
	Logger *slog.Logger
}

// NewNATSIngester dials NATS and prepares the subscriber. Call Run
// to start dispatching messages ; Close to release subscriptions
// and the connection.
func NewNATSIngester(opts NATSOptions) (*NATSIngester, error) {
	if opts.URL == "" {
		return nil, errors.New("ingest: NATS URL required")
	}
	if opts.Sink == nil {
		return nil, errors.New("ingest: Sink required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if len(opts.Subjects) == 0 {
		opts.Subjects = []string{"weft.>"}
	}
	conn, err := nats.Connect(opts.URL,
		nats.Name("weft-doctor"),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, err
	}
	return &NATSIngester{
		conn:     conn,
		subjects: opts.Subjects,
		sink:     opts.Sink,
		log:      opts.Logger,
	}, nil
}

// Run subscribes to each configured subject and blocks until ctx is
// cancelled. The actual message handling runs on NATS-internal
// goroutines ; Run only signals readiness and waits.
func (n *NATSIngester) Run(ctx context.Context) error {
	for _, subj := range n.subjects {
		s, err := n.conn.Subscribe(subj, n.onMsg)
		if err != nil {
			return err
		}
		n.subs = append(n.subs, s)
		n.log.Info("subscribed", "subject", subj)
	}
	<-ctx.Done()
	return ctx.Err()
}

// Close drops all subscriptions and the connection. Safe to call
// multiple times.
func (n *NATSIngester) Close() {
	for _, s := range n.subs {
		_ = s.Unsubscribe()
	}
	n.subs = nil
	if n.conn != nil {
		n.conn.Drain() //nolint:errcheck
	}
}

func (n *NATSIngester) onMsg(msg *nats.Msg) {
	ev, ok := parseSlog(msg.Data)
	if !ok {
		// Not a slog record — could be a status ping or a binary blob.
		// Silently drop ; logging on each non-slog msg would itself
		// flood the buffer in busy clusters.
		return
	}
	if ev.Level != "WARN" && ev.Level != "ERROR" {
		// V0.1 only buffers warnings + errors. INFO/DEBUG are kept
		// for human tailing but not for LLM classification.
		return
	}
	ev.Source = msg.Subject
	n.sink.Add(ev)
}

// parseSlog decodes a JSON slog record. slog.JSONHandler emits the
// standard fields (time, level, msg) at the top level + arbitrary
// extra fields ; we collect the extras into Attrs.
func parseSlog(data []byte) (classify.LogEvent, bool) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return classify.LogEvent{}, false
	}
	ev := classify.LogEvent{
		Attrs: map[string]any{},
	}
	for k, v := range raw {
		switch k {
		case "time":
			if s, ok := v.(string); ok {
				_ = ev.Time.UnmarshalText([]byte(s))
			}
		case "level":
			if s, ok := v.(string); ok {
				ev.Level = s
			}
		case "msg":
			if s, ok := v.(string); ok {
				ev.Msg = s
			}
		default:
			ev.Attrs[k] = v
		}
	}
	if ev.Level == "" || ev.Msg == "" {
		return classify.LogEvent{}, false
	}
	if len(ev.Attrs) == 0 {
		ev.Attrs = nil
	}
	return ev, true
}
