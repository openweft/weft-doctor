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
	// Journal : V0.2 systemd-journal tailer. Empty Units = disabled,
	// the V0.1 NATS-only ingest path stays the only source.
	Journal JournalConfig
}

// NATSConfig : connection + subscription set.
type NATSConfig struct {
	URL      string   `hcl:"url"`
	Subjects []string `hcl:"subjects,optional"`
}

// JournalConfig : V0.2 systemd-journal tailer. Operators enable by
// listing one or more systemd units in `units = […]`. The tailer
// spawns `journalctl --output=json -f -u <unit>` per entry, filters
// crash-relevant lines (Go panic, SIGKILL, OOM, segfault, fatal),
// and feeds them into the same buffer the NATS ingest uses. Empty
// units list = tailer disabled (V0.1 NATS-only behaviour preserved).
type JournalConfig struct {
	Units []string `hcl:"units,optional"` // ["weft-agent.service", ...]
	Host  string   `hcl:"host,optional"`  // defaults to os.Hostname()
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

// hclConfig mirrors Config with HCL struct tags. Public types stay
// format-agnostic ; this private mirror does the decode dance.
// Pointer types on optional blocks let HCL skip them entirely so a
// minimal config file with only `nats { … }` parses cleanly.
type hclConfig struct {
	NATS             NATSConfig     `hcl:"nats,block"`
	Ollama           *OllamaConfig  `hcl:"ollama,block"`
	Buffer           *BufferConfig  `hcl:"buffer,block"`
	Journal          *JournalConfig `hcl:"journal,block"`
	DispatchInterval string         `hcl:"dispatch_interval,optional"`
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
		NATS: raw.NATS,
	}
	if raw.Ollama != nil {
		cfg.Ollama = *raw.Ollama
	}
	if raw.Buffer != nil {
		cfg.Buffer = *raw.Buffer
	}
	if raw.Journal != nil {
		cfg.Journal = *raw.Journal
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
