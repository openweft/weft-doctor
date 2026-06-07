# weft-doctor

AI-driven log triage for the openweft control plane. Subscribes to
NATS, classifies error/warning bursts via a local Ollama LLM, and
publishes structured diagnoses on a NATS subject for downstream
consumers (weft-webui's Diagnoses panel, alerting bridges, future
remediation gates).

V0.1 is **passive** : observe and explain. No remediation.

## Pipeline

```
NATS (weft.>)
   │
   ▼
ingest.NATSIngester    parse slog JSON, filter WARN+ERROR
   │
   ▼
buffer.Buffer          sliding window, dedup by (level, msg), burst trigger
   │
   ▼
dispatch loop          poll every dispatch_interval, batch ready signatures
   │
   ▼
llm.OllamaClient       classify batch -> []Diagnosis (local Ollama, no cloud)
   │
   ▼
output.NATSSink        publish to weft.diagnosis.<severity>.<hash>
```

## Why NATS-only (no PAT, no GitHub creds)

Authentication boundaries stay with whoever the consumer is already
authenticated against : weft-webui via dex, alerting via its own
mTLS, future remediation gates via the cluster's existing trust
chain. weft-doctor itself never holds credentials.

The dashboard UX (Renovate-style aggregated view, dedup by pattern
hash) belongs in weft-webui where the operator already lives — see
the follow-up issue for the `Diagnoses` panel.

## Why Ollama (no cloud dep)

openweft's policy is self-contained, air-gappable. Ollama runs as a
microVM alongside the rest of the control plane (next to etcd, NATS,
dex, zot). No cloud round-trip, no API key on disk, no rate-limit
spikes when a real incident floods the buffer.

Recommended model : `llama3.1:8b` for laptops / dev, `llama3.1:70b` (or
`qwen2.5:32b`) for clusters where the operator wants higher classification
quality and has the GPU budget.

## Configuration

HCL (mirrors `cluster.hcl` / `weft-firstboot` convention) :

```hcl
nats {
  url      = "nats://nats.weft.svc:4222"
  subjects = ["weft.agent.>", "weft.microvm.>", "weft.ha.>"]
}

ollama {
  url     = "http://ollama.weft.svc:11434"
  model   = "llama3.1:8b"
  timeout = "60s"
}

buffer {
  window          = "5m"
  burst_threshold = 10
}

dispatch_interval = "15s"
```

Run :

```
weft-doctor run --config /etc/weft-doctor/config.hcl
```

No environment secrets. No PAT. No KMS. The NATS connection itself
inherits auth from the cluster's NATS config (TLS, NKEYS, JWT —
whatever the operator already runs).

## Output schema

Each Diagnosis is published as JSON on subject :

```
weft.diagnosis.<severity>.<pattern_hash>
```

Where `<severity>` ∈ {critical, high, medium, low} so consumers can
filter via wildcards :

- `weft.diagnosis.critical.>` — only paging-worthy
- `weft.diagnosis.>` — everything

Schema (see `classify/types.go` for the Go contract) :

```json
{
  "pattern_hash": "abc123",
  "severity": "critical",
  "title": "primary postgres lost on dc2",
  "root_cause": "disk full on /var/lib/pgsql triggers fatal exit",
  "suggested_action": "df -h on dc2-r1-h1, free space, then rcctl restart postgres",
  "file_location": "internal/postgres/controller.go:142",
  "occurrences": 17,
  "first_seen": "2026-06-07T19:50Z",
  "last_seen": "2026-06-07T20:32Z",
  "examples": [...]
}
```

## What's NOT in V0.1

- Active remediation (restart VM, rollback, scale)
- LLM providers other than Ollama (Anthropic / OpenAI / vLLM)
- Datasources other than NATS (file tail, gRPC stream, journald)
- Aggregated dashboard UI (belongs in weft-webui — separate follow-up)
- Code-fix PR suggestions

These are additive — file an issue when needed.

## License

BSD 3-Clause — see LICENSE.
