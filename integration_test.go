// integration_test.go drives the full diagnosis pipeline end-to-end
// without leaving the test process : embedded NATS server + httptest
// Ollama stub + a slog producer wired via weft-slognats + a real
// weft-doctor ingest/buffer/dispatch/output stack.
//
// Sequence under test :
//
//	slog.Error(...) on a weft-slognats handler
//	  -> NATS subject weft.testcomp.host01.log
//	weft-doctor's NATSIngester parses the JSON record
//	  -> buffer.Add (dedup by Level+Msg)
//	burst threshold crossed
//	  -> dispatcher batches + calls Ollama stub
//	Ollama stub returns canned Diagnosis JSON
//	  -> outputs publish on weft.diagnosis.critical.<hash>
//	test subscriber asserts the message arrived
//
// This is the contract-validation we couldn't get from unit tests
// alone (they used fakes for NATS + Ollama). If this test ever
// breaks, the pipeline is broken — the file:line in the failure
// tells you which seam to investigate.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	weftslognats "github.com/openweft/weft-slognats"

	"github.com/openweft/weft-doctor/buffer"
	"github.com/openweft/weft-doctor/classify"
	"github.com/openweft/weft-doctor/ingest"
	"github.com/openweft/weft-doctor/llm"
	"github.com/openweft/weft-doctor/output"
)

// TestEndToEndPipeline is the headline integration test. Runs in
// under a second on a healthy box ; the heavy lifting is the embedded
// NATS + httptest spin-up.
func TestEndToEndPipeline(t *testing.T) {
	// --- 1. Embedded NATS server ---------------------------------
	natsURL := startEmbeddedNATS(t)

	// --- 2. Ollama stub returns one canned Diagnosis -------------
	ollamaURL := startOllamaStub(t, []classify.Diagnosis{{
		PatternHash:     "deadbeef",
		Severity:        classify.SeverityCritical,
		Title:           "Postgres primary fenced unexpectedly",
		RootCause:       "disk full on /var/lib/pgsql",
		SuggestedAction: "df -h on dc1-r1-h1 + free space + rcctl restart postgres",
		FileLocation:    "internal/postgres/controller.go:142",
		Occurrences:     3,
	}})

	// --- 3. Diagnosis subscriber (test observer) -----------------
	subscriber, err := nats.Connect(natsURL, nats.Name("test-subscriber"))
	if err != nil {
		t.Fatalf("subscriber connect : %v", err)
	}
	defer subscriber.Drain() //nolint:errcheck

	gotDiag := make(chan classify.Diagnosis, 4)
	sub, err := subscriber.Subscribe("weft.diagnosis.>", func(msg *nats.Msg) {
		var d classify.Diagnosis
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Errorf("subscriber decode : %v", err)
			return
		}
		gotDiag <- d
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe() //nolint:errcheck

	// --- 4. Producer side : weft-slognats handler ---------------
	t.Setenv(weftslognats.EnvNATSURL, natsURL)
	prodLog, prodCloser := weftslognats.SetupFromEnv("weft.testcomp.host01.log")
	defer prodCloser.Close()

	// --- 5. Consumer side : weft-doctor wired against same NATS --
	buf := buffer.New(buffer.Options{
		Window:         5 * time.Second,
		BurstThreshold: 3, // small so the test isn't slow
	})
	ing, err := ingest.NewNATSIngester(ingest.NATSOptions{
		URL:      natsURL,
		Subjects: []string{"weft.>"},
		Sink:     buf,
		Logger:   slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ing.Close()

	llmClient := llm.NewOllamaClient(llm.OllamaOptions{
		URL:     ollamaURL,
		Model:   "test-model",
		Timeout: 5 * time.Second,
	})

	outConn, err := nats.Connect(natsURL, nats.Name("test-output"))
	if err != nil {
		t.Fatal(err)
	}
	defer outConn.Drain() //nolint:errcheck
	multi := &output.Multi{Sinks: []output.Sink{output.NewNATSSink(outConn, "")}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run ingest in background.
	go func() {
		_ = ing.Run(ctx)
	}()
	// Give the subscription a moment to register.
	time.Sleep(50 * time.Millisecond)

	// --- 6. Producer publishes 3 ERROR records (above threshold) -
	for i := 0; i < 3; i++ {
		prodLog.Error("primary fenced", "vm_uuid", "abc", "attempt", i)
	}

	// Wait for ingest to receive them ; small busy-loop bounded
	// by a deadline.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		batch := buf.ReadyBatch(10)
		if len(batch) >= 1 {
			// Classify + publish manually (we don't run the full
			// dispatcher loop ; that's the cmd/ binary's job).
			diags, err := llmClient.Classify(ctx, batch)
			if err != nil {
				t.Fatalf("classify : %v", err)
			}
			if len(diags) == 0 {
				t.Fatal("Ollama stub returned no diagnoses")
			}
			if errs := multi.Publish(ctx, diags); len(errs) > 0 {
				t.Fatalf("output publish : %v", errs)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// --- 7. Subscriber must receive the diagnosis ----------------
	select {
	case d := <-gotDiag:
		if d.PatternHash != "deadbeef" {
			t.Errorf("diagnosis pattern_hash = %q ; want deadbeef", d.PatternHash)
		}
		if d.Severity != classify.SeverityCritical {
			t.Errorf("severity = %q ; want critical", d.Severity)
		}
		if !strings.Contains(d.Title, "Postgres") {
			t.Errorf("title = %q ; want substring 'Postgres'", d.Title)
		}
		t.Logf("end-to-end pipeline validated : Diagnosis arrived on NATS")
	case <-time.After(3 * time.Second):
		t.Fatal("no diagnosis published to weft.diagnosis.> within 3s")
	}
}

// startEmbeddedNATS spins up a NATS server on a random port for the
// duration of the test. Returns the dial URL.
func startEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:           "127.0.0.1",
		Port:           -1, // random
		NoLog:          true,
		NoSigs:         true,
		MaxControlLine: 4096,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(3 * time.Second) {
		t.Fatal("nats server not ready in 3s")
	}
	t.Cleanup(func() { srv.Shutdown() })
	return srv.ClientURL()
}

// startOllamaStub serves /api/chat returning a hard-coded JSON
// response shaped like Ollama's actual output. Only "message.content"
// is consumed by the client ; everything else can be empty.
func startOllamaStub(t *testing.T, diags []classify.Diagnosis) string {
	t.Helper()
	var calls sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Lock()
		defer calls.Unlock()
		// The OllamaClient sends to /api/chat ; everything else 404.
		if r.URL.Path != "/api/chat" {
			http.NotFound(w, r)
			return
		}
		// Marshal the diagnoses into the wire shape the client
		// expects : {"message":{"role":"assistant","content":"<json>"}}
		payload, _ := json.Marshal(struct {
			Diagnoses []classify.Diagnosis `json:"diagnoses"`
		}{Diagnoses: diags})
		response := struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		}{}
		response.Message.Role = "assistant"
		response.Message.Content = string(payload)
		_ = json.NewEncoder(w).Encode(response)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestPipelineDegradedWhenOllamaUnreachable proves that an Ollama
// outage doesn't break the pipeline (the dispatcher logs the error
// and retries next tick). We don't assert the retry behaviour here ;
// just that Classify returns an error rather than a panic.
func TestPipelineDegradedWhenOllamaUnreachable(t *testing.T) {
	llmClient := llm.NewOllamaClient(llm.OllamaOptions{
		URL:     "http://127.0.0.1:1", // certainly not listening
		Model:   "test-model",
		Timeout: 200 * time.Millisecond,
	})
	_, err := llmClient.Classify(context.Background(), []classify.LogEvent{
		{Level: "ERROR", Msg: "test"},
	})
	if err == nil {
		t.Fatal("unreachable Ollama should error")
	}
	if !strings.Contains(err.Error(), "ollama") && !strings.Contains(err.Error(), "connection") {
		t.Errorf("err = %v ; want it to mention ollama or connection", err)
	}
}

// keep all imports live in case future refactor removes their first
// use ; better caught at compile than at runtime.
var (
	_ = fmt.Sprintf
)
