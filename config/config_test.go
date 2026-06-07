package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_FullExample(t *testing.T) {
	src := `
nats {
  url      = "nats://nats.weft.svc:4222"
  subjects = ["weft.agent.>", "weft.microvm.>"]
}

ollama {
  url     = "http://ollama.weft.svc:11434"
  model   = "llama3.1:8b"
  timeout = "45s"
}

buffer {
  window          = "5m"
  burst_threshold = 10
}

dispatch_interval = "15s"

target "openweft/weft" {
  subjects = ["weft.agent.>"]
}

target "openweft/weft-ha-postgresql" {
  subjects = ["weft.ha.postgres.>"]
}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.hcl")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NATS.URL != "nats://nats.weft.svc:4222" {
		t.Errorf("NATS.URL = %q", cfg.NATS.URL)
	}
	if len(cfg.NATS.Subjects) != 2 {
		t.Errorf("NATS.Subjects = %v", cfg.NATS.Subjects)
	}
	if cfg.Ollama.Model != "llama3.1:8b" {
		t.Errorf("Ollama.Model = %q", cfg.Ollama.Model)
	}
	if cfg.OllamaTimeout() != 45*time.Second {
		t.Errorf("OllamaTimeout = %v", cfg.OllamaTimeout())
	}
	if cfg.BufferWindow() != 5*time.Minute {
		t.Errorf("BufferWindow = %v", cfg.BufferWindow())
	}
	if cfg.Buffer.BurstThreshold != 10 {
		t.Errorf("BurstThreshold = %d", cfg.Buffer.BurstThreshold)
	}
	if cfg.DispatchInterval != 15*time.Second {
		t.Errorf("DispatchInterval = %v", cfg.DispatchInterval)
	}
	if len(cfg.Targets) != 2 {
		t.Fatalf("Targets = %d", len(cfg.Targets))
	}
	if cfg.Targets[0].Repo != "openweft/weft" {
		t.Errorf("Target[0].Repo = %q", cfg.Targets[0].Repo)
	}
}

func TestLoad_MinimalDefaults(t *testing.T) {
	src := `nats { url = "nats://x:4222" }`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.hcl")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.NATS.Subjects) != 1 || cfg.NATS.Subjects[0] != "weft.>" {
		t.Errorf("default Subjects = %v ; want [weft.>]", cfg.NATS.Subjects)
	}
	if cfg.DispatchInterval != 10*time.Second {
		t.Errorf("default DispatchInterval = %v ; want 10s", cfg.DispatchInterval)
	}
	if cfg.BufferWindow() != 5*time.Minute {
		t.Errorf("default BufferWindow = %v", cfg.BufferWindow())
	}
	if cfg.OllamaTimeout() != 60*time.Second {
		t.Errorf("default OllamaTimeout = %v", cfg.OllamaTimeout())
	}
}

func TestLoad_NoURL_Error(t *testing.T) {
	src := `nats { subjects = ["weft.>"] }`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.hcl")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Error("expected error when nats.url missing")
	}
}
