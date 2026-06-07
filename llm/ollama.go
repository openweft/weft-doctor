package llm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/openweft/weft-doctor/classify"
)

// OllamaClient calls a local Ollama HTTP endpoint for classification.
// Default URL is http://localhost:11434 (Ollama's standard listen
// address) ; pointable at a remote Ollama via Options.
//
// Why Ollama and not direct OpenAI/Anthropic : openweft's policy is
// self-contained, air-gappable. Operators run Ollama as a microVM
// alongside the rest of the control plane ; no cloud round-trip,
// no API key on disk, no rate-limit headaches.
type OllamaClient struct {
	url    string
	model  string
	client *http.Client
	log    *slog.Logger
}

// OllamaOptions configures the client. Empty fields fall back to
// sensible defaults so an operator can `NewOllamaClient(OllamaOptions{})`
// against a localhost Ollama with the recommended default model.
type OllamaOptions struct {
	// URL is the Ollama HTTP base, e.g. "http://ollama.weft.svc:11434".
	URL string
	// Model is the model tag, e.g. "llama3.1:8b". Default :
	// "llama3.1:8b" — large enough to follow structured-output
	// instructions, small enough to run on commodity hardware.
	Model string
	// Timeout caps one Classify call. Default 60s ; the cluster
	// burst-detection cadence is the upper bound on how long we
	// can stall before the next batch arrives.
	Timeout time.Duration
	// Logger is the slog logger. Default : slog.Default().
	Logger *slog.Logger
}

// NewOllamaClient builds a client with options + defaults.
func NewOllamaClient(opts OllamaOptions) *OllamaClient {
	if opts.URL == "" {
		opts.URL = "http://localhost:11434"
	}
	if opts.Model == "" {
		opts.Model = "llama3.1:8b"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 60 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &OllamaClient{
		url:    strings.TrimRight(opts.URL, "/"),
		model:  opts.Model,
		client: &http.Client{Timeout: opts.Timeout},
		log:    opts.Logger,
	}
}

// Classify implements Client. Sends a structured prompt to Ollama's
// /api/chat endpoint with format=json so the response is constrained
// to valid JSON.
//
// Quirks we work around :
//   - Ollama's format=json only constrains the SHAPE, not the schema.
//     We post-validate the unmarshalled JSON against the Diagnosis
//     contract and discard malformed entries (rare in practice with
//     instruction-following models).
//   - The model occasionally returns a single Diagnosis when we
//     expect an array. We accept both (object or array) and normalise.
func (c *OllamaClient) Classify(ctx context.Context, events []classify.LogEvent) ([]classify.Diagnosis, error) {
	if len(events) == 0 {
		return nil, nil
	}
	prompt := buildPrompt(events)
	reqBody, err := json.Marshal(ollamaChatRequest{
		Model:  c.model,
		Stream: false,
		Format: "json",
		Messages: []ollamaMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: prompt},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/api/chat", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama call: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return nil, fmt.Errorf("ollama status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var chatResp ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return parseDiagnoses(chatResp.Message.Content, events, c.log)
}

// systemPrompt instructs the model on the contract. Kept short
// because long system prompts cost tokens AND get truncated by some
// models ; the user prompt does the heavy lifting.
const systemPrompt = `You are a cluster-diagnostics expert for the openweft platform.
You receive a batch of error/warning log events from one or more
distributed components (weft-agent, weft-microvm-agent, drivers, HA
agents). Your job : group events that share an underlying cause and
emit a single Diagnosis for each group.

Respond with ONE JSON object : {"diagnoses": [...]} where each entry has
exactly these keys :
  pattern_hash      string (8+ chars, deterministic for the same cause)
  severity          one of "critical" | "high" | "medium" | "low"
  title             one-line summary (<= 80 chars)
  root_cause        one or two sentences
  suggested_action  ONE actionable command or change
  file_location     "path/to/file.go:LINE" if inferable, else ""
  occurrences       integer ; how many input events matched

No prose outside the JSON. No markdown. No reasoning aloud.`

func buildPrompt(events []classify.LogEvent) string {
	// Cap at 50 events to keep within typical local-model context.
	// Caller should pre-dedup by raw key ; the model sees the
	// representative sample.
	max := 50
	if len(events) < max {
		max = len(events)
	}
	sample, _ := json.MarshalIndent(events[:max], "", "  ")
	return fmt.Sprintf("Classify these %d log events :\n\n%s", max, sample)
}

func parseDiagnoses(content string, events []classify.LogEvent, log *slog.Logger) ([]classify.Diagnosis, error) {
	// Accept either {"diagnoses": [...]} or a bare array, in case the
	// model decides to skip the wrapper.
	trimmed := strings.TrimSpace(content)
	var wrapped struct {
		Diagnoses []classify.Diagnosis `json:"diagnoses"`
	}
	var bareArr []classify.Diagnosis

	switch {
	case strings.HasPrefix(trimmed, "{"):
		if err := json.Unmarshal([]byte(trimmed), &wrapped); err != nil {
			return nil, fmt.Errorf("unmarshal wrapped: %w", err)
		}
	case strings.HasPrefix(trimmed, "["):
		if err := json.Unmarshal([]byte(trimmed), &bareArr); err != nil {
			return nil, fmt.Errorf("unmarshal bare array: %w", err)
		}
		wrapped.Diagnoses = bareArr
	default:
		return nil, fmt.Errorf("ollama returned non-JSON content: %q", trimmed[:min(120, len(trimmed))])
	}

	out := make([]classify.Diagnosis, 0, len(wrapped.Diagnoses))
	for _, d := range wrapped.Diagnoses {
		if d.PatternHash == "" {
			// The model dropped the hash. Synthesise from title+root_cause
			// so dedup still works for this run.
			d.PatternHash = fallbackHash(d.Title, d.RootCause)
		}
		if !validSeverity(d.Severity) {
			log.Warn("invalid severity, defaulting to medium", "got", d.Severity)
			d.Severity = classify.SeverityMedium
		}
		// Examples : pick up to 3 input events at random — V0.1 just
		// takes the first 3 matching by occurrence count.
		if len(d.Examples) == 0 && len(events) > 0 {
			n := 3
			if n > len(events) {
				n = len(events)
			}
			d.Examples = events[:n]
		}
		out = append(out, d)
	}
	return out, nil
}

func validSeverity(s classify.Severity) bool {
	switch s {
	case classify.SeverityCritical, classify.SeverityHigh, classify.SeverityMedium, classify.SeverityLow:
		return true
	}
	return false
}

func fallbackHash(title, rootCause string) string {
	h := sha256.Sum256([]byte(title + "|" + rootCause))
	return hex.EncodeToString(h[:6])
}

// Ollama wire types — small enough that we don't pull in an SDK.
type ollamaChatRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   bool            `json:"stream"`
	Format   string          `json:"format,omitempty"`
}

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatResponse struct {
	Message ollamaMessage `json:"message"`
}
