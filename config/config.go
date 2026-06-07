// Package config parses weft-doctor's HCL configuration file.
// Mirrors the openweft convention (HCL primary ; cluster.hcl /
// weft-firstboot / etc.).
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
)

// Config is the parsed, validated configuration.
type Config struct {
	// NATS connection : URL + subjects to subscribe to.
	NATS NATSConfig
	// Ollama LLM provider config (V0.1 only provider).
	Ollama OllamaConfig
	// Buffer window + burst threshold for the rate-limiter between
	// ingest and the LLM.
	Buffer BufferConfig
	// Dispatch cadence : how often the dispatcher polls ReadyBatch
	// from the buffer.
	DispatchInterval time.Duration
	// Targets is the list of GitHub repos that receive a
	// Dashboard issue (one repo per target). Optional ; without
	// targets the binary only publishes to NATS.
	Targets []Target
}

// NATSConfig : connection + subscription set.
type NATSConfig struct {
	URL      string   `hcl:"url"`
	Subjects []string `hcl:"subjects,optional"`
}

// OllamaConfig : endpoint + model.
type OllamaConfig struct {
	URL     string `hcl:"url,optional"`
	Model   string `hcl:"model,optional"`
	Timeout string `hcl:"timeout,optional"` // "60s" form ; parsed at load.
}

// BufferConfig : sliding window + burst trigger.
type BufferConfig struct {
	Window         string `hcl:"window,optional"`          // "5m"
	BurstThreshold int    `hcl:"burst_threshold,optional"` // 10
}

// Target is one GitHub repo that should receive a Dashboard issue.
// Subjects filters which Diagnoses end up on this repo's issue based
// on the source NATS subject they came from (allows fanning, e.g.
// weft.agent.> on openweft/weft, weft.ha.postgres.> on
// openweft/weft-ha-postgresql).
type Target struct {
	Repo     string   `hcl:"repo,label"` // "openweft/weft"
	Subjects []string `hcl:"subjects,optional"`
}

// hclConfig mirrors Config with HCL struct tags. Public types stay
// format-agnostic ; this private mirror does the decode dance.
// Pointer types on optional blocks let HCL skip them entirely so a
// minimal config file with only `nats { … }` parses cleanly.
type hclConfig struct {
	NATS             NATSConfig    `hcl:"nats,block"`
	Ollama           *OllamaConfig `hcl:"ollama,block"`
	Buffer           *BufferConfig `hcl:"buffer,block"`
	DispatchInterval string        `hcl:"dispatch_interval,optional"`
	Targets          []Target      `hcl:"target,block"`
}

// Load reads + parses the file. Defaults are applied here so the
// command package gets a fully-populated Config.
func Load(path string) (Config, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	p := hclparse.NewParser()
	f, diags := p.ParseHCL(src, path)
	if diags.HasErrors() {
		return Config{}, fmt.Errorf("parse: %s", diags.Error())
	}
	var raw hclConfig
	if diags := gohcl.DecodeBody(f.Body, nil, &raw); diags.HasErrors() {
		return Config{}, fmt.Errorf("decode: %s", diags.Error())
	}
	cfg := Config{
		NATS:    raw.NATS,
		Targets: raw.Targets,
	}
	if raw.Ollama != nil {
		cfg.Ollama = *raw.Ollama
	}
	if raw.Buffer != nil {
		cfg.Buffer = *raw.Buffer
	}
	if raw.DispatchInterval != "" {
		d, err := time.ParseDuration(raw.DispatchInterval)
		if err != nil {
			return Config{}, fmt.Errorf("dispatch_interval: %w", err)
		}
		cfg.DispatchInterval = d
	}
	if cfg.DispatchInterval == 0 {
		cfg.DispatchInterval = 10 * time.Second
	}
	if cfg.NATS.URL == "" {
		return Config{}, fmt.Errorf("nats.url required")
	}
	if len(cfg.NATS.Subjects) == 0 {
		cfg.NATS.Subjects = []string{"weft.>"}
	}
	return cfg, nil
}

// BufferWindow parses cfg.Buffer.Window with a sensible default.
func (c Config) BufferWindow() time.Duration {
	if c.Buffer.Window == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(c.Buffer.Window)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

// OllamaTimeout parses cfg.Ollama.Timeout with default 60s.
func (c Config) OllamaTimeout() time.Duration {
	if c.Ollama.Timeout == "" {
		return 60 * time.Second
	}
	d, err := time.ParseDuration(c.Ollama.Timeout)
	if err != nil {
		return 60 * time.Second
	}
	return d
}
