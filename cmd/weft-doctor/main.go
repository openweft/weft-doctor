// Command weft-doctor classifies live error/warning logs from the
// openweft control plane via a local LLM (Ollama) and surfaces
// findings as :
//
//   - structured NATS messages on weft.diagnosis.<severity>.<hash>
//   - a single auto-maintained Dashboard issue per target GitHub repo
//
// V0.1 is PASSIVE : it observes and explains, it never takes
// remediative actions. V0.5+ may add operator-approved auto-fix
// flows behind explicit cluster.hcl gates.
//
// Architecture :
//
//	NATS (weft.>)
//	   │
//	   ▼
//	ingest.NATSIngester ── parses slog JSON, filters WARN+ERROR
//	   │
//	   ▼
//	buffer.Buffer ── sliding window, dedup by (level, msg), burst trigger
//	   │
//	   ▼
//	dispatch loop ── poll ReadyBatch() every dispatch_interval
//	   │
//	   ▼
//	llm.OllamaClient ── classify batch → []Diagnosis
//	   │
//	   ▼
//	output.Multi (NATSSink + GitHubSink per target)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"

	"github.com/openweft/weft-doctor/buffer"
	"github.com/openweft/weft-doctor/classify"
	"github.com/openweft/weft-doctor/config"
	"github.com/openweft/weft-doctor/ingest"
	"github.com/openweft/weft-doctor/llm"
	"github.com/openweft/weft-doctor/output"
)

// Build metadata, injected via -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "weft-doctor",
		Short:        "AI-driven log triage for the openweft control plane",
		Long:         "weft-doctor subscribes to NATS, classifies error/warning bursts via a local Ollama LLM, and surfaces findings as NATS diagnoses + a per-repo GitHub Dashboard issue. Passive in V0.1 — no remediation.",
		SilenceUsage: true,
	}
	root.AddCommand(versionCmd(), runCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "weft-doctor %s (commit %s, built %s)\n", version, commit, date)
			return err
		},
	}
}

func runCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Subscribe + classify + dispatch (long-running)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return fmt.Errorf("config: %w", err)
			}
			return run(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "/etc/weft-doctor/config.hcl", "path to the HCL configuration file")
	return cmd
}

func run(ctx context.Context, cfg config.Config) error {
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Buffer : the sliding window between ingest and dispatch.
	buf := buffer.New(buffer.Options{
		Window:         cfg.BufferWindow(),
		BurstThreshold: cfg.Buffer.BurstThreshold,
	})

	// 2. Ingest : NATS subscriber, parses JSON slog, feeds buf.
	ing, err := ingest.NewNATSIngester(ingest.NATSOptions{
		URL:      cfg.NATS.URL,
		Subjects: cfg.NATS.Subjects,
		Sink:     buf,
		Logger:   log,
	})
	if err != nil {
		return fmt.Errorf("ingest: %w", err)
	}
	defer ing.Close()

	// 3. LLM : Ollama client.
	llmClient := llm.NewOllamaClient(llm.OllamaOptions{
		URL:     cfg.Ollama.URL,
		Model:   cfg.Ollama.Model,
		Timeout: cfg.OllamaTimeout(),
		Logger:  log,
	})

	// 4. Output sinks : NATS + per-target GitHub.
	natsConn, err := nats.Connect(cfg.NATS.URL, nats.Name("weft-doctor-out"))
	if err != nil {
		return fmt.Errorf("output nats connect: %w", err)
	}
	defer natsConn.Drain() //nolint:errcheck

	multi := &output.Multi{Sinks: []output.Sink{output.NewNATSSink(natsConn, "")}}

	pat := os.Getenv("WEFT_DOCTOR_GH_PAT")
	for _, t := range cfg.Targets {
		if pat == "" {
			log.Warn("WEFT_DOCTOR_GH_PAT unset ; skipping GitHub target", "repo", t.Repo)
			continue
		}
		owner, repo, ok := splitRepo(t.Repo)
		if !ok {
			log.Warn("malformed target.repo, expecting owner/name", "got", t.Repo)
			continue
		}
		ghSink, err := output.NewGitHubSink(output.GitHubOptions{
			Token:  pat,
			Owner:  owner,
			Repo:   repo,
			Logger: log,
		})
		if err != nil {
			log.Warn("GitHub sink init failed", "repo", t.Repo, "err", err)
			continue
		}
		multi.Sinks = append(multi.Sinks, ghSink)
	}

	// 5. Start ingest in the background, then enter the dispatch loop.
	ingErr := make(chan error, 1)
	go func() { ingErr <- ing.Run(ctx) }()

	log.Info("weft-doctor running",
		"nats", cfg.NATS.URL,
		"subjects", cfg.NATS.Subjects,
		"ollama", cfg.Ollama.URL,
		"buffer_window", cfg.BufferWindow(),
		"burst_threshold", cfg.Buffer.BurstThreshold,
		"dispatch_interval", cfg.DispatchInterval,
		"sinks", len(multi.Sinks))

	return dispatchLoop(ctx, buf, llmClient, multi, cfg.DispatchInterval, log, ingErr)
}

// dispatchLoop polls the buffer on a tick. On each tick, ReadyBatch
// returns whatever signatures have crossed the burst threshold since
// the last emission. We send them to the LLM, enrich with timing
// from buffer.Lookup, fan out to sinks.
func dispatchLoop(
	ctx context.Context,
	buf *buffer.Buffer,
	llmClient llm.Client,
	out *output.Multi,
	interval time.Duration,
	log *slog.Logger,
	ingErr <-chan error,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-ingErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("ingest exited: %w", err)
			}
			return nil
		case <-ticker.C:
			batch := buf.ReadyBatch(20)
			if len(batch) == 0 {
				continue
			}
			log.Info("dispatching", "batch_size", len(batch))
			diags, err := llmClient.Classify(ctx, batch)
			if err != nil {
				log.Warn("classify failed (retry next tick)", "err", err)
				continue
			}
			if len(diags) == 0 {
				continue
			}
			// Enrich with buffer-side timing on each diagnosis.
			for i := range diags {
				// One example event per diagnosis is enough to recover
				// the signature ; ReadyBatch returned the sample.
				if i < len(batch) {
					sig := buffer.Signature(batch[i])
					if first, last, count, ok := buf.Lookup(sig); ok {
						diags[i].FirstSeen = first
						diags[i].LastSeen = last
						if diags[i].Occurrences == 0 {
							diags[i].Occurrences = count
						}
					}
				}
			}
			for _, err := range out.Publish(ctx, diags) {
				log.Warn("sink publish failed", "err", err)
			}
		}
	}
}

// splitRepo parses "owner/name" into the two halves. Returns ok=false
// for malformed input.
func splitRepo(s string) (owner, name string, ok bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			if i == 0 || i == len(s)-1 {
				return "", "", false
			}
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// Compile-time check that classify.LogEvent stays exported as
// expected by reasonable downstream consumers (ingest uses it as
// the value type for Sink.Add).
var _ = classify.LogEvent{}
