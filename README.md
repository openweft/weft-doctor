# weft-doctor

AI-driven log triage for the openweft control plane. Subscribes to
NATS, classifies error/warning bursts via a local Ollama LLM, and
surfaces findings as structured NATS messages plus an auto-maintained
Dependency-Dashboard-style issue per target GitHub repo.

V0.1 is **passive** : it observes and explains. It never takes
remediative actions. Active flows land in V0.5+ behind explicit
cluster-config gates.

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
output.Multi
   ├─ NATSSink         publish to weft.diagnosis.<severity>.<hash>
   └─ GitHubSink       maintain ONE Dashboard issue per target repo
```

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

# Per-repo Dashboard issues. Empty `subjects` means "all diagnoses".
target "openweft/weft" {
  subjects = ["weft.agent.>"]
}

target "openweft/weft-ha-postgresql" {
  subjects = ["weft.ha.postgres.>"]
}
```

GitHub PAT is read from the environment, never from disk :

```
export WEFT_DOCTOR_GH_PAT=ghp_xxxxxxxxxxxxx
weft-doctor run --config /etc/weft-doctor/config.hcl
```

PAT scopes required : `repo` (private repos) or `public_repo` + `issues`
on public repos.

## Dashboard issue format

```markdown
# Cluster Diagnosis Dashboard

Auto-maintained by weft-doctor. Last update: 2026-06-07T20:32Z. Tracking 7 active patterns.

## 🔴 [critical] primary postgres lost on dc2

**Root cause** : disk full on /var/lib/pgsql triggers fatal exit
**Suggested action** : df -h on dc2-r1-h1, free space, then rcctl restart postgres
**Likely location** : `internal/postgres/controller.go:142`

**Pattern** : `abc123` · **Occurrences** : 17 · **First seen** : 2026-06-07T19:50Z · **Last seen** : 2026-06-07T20:32Z

<details><summary>Example events</summary>

```
[ERROR] FATAL: disk full (subject: weft.agent.dc2-r1-h1)
```

</details>

## 🟡 [medium] driver plugin handshake timeouts

...
```

Sorted by severity desc, then occurrences desc. Capped to the
configured `max_recent` (default 20).

## What's NOT in V0.1

- Active remediation (restart VM, rollback, scale)
- LLM providers other than Ollama (Anthropic / OpenAI / vLLM)
- Datasources other than NATS (file tail, gRPC stream, journald)
- Code-fix PR suggestions

These are additive — file an issue when needed.

## License

BSD 3-Clause — see LICENSE.
