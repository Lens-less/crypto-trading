# Crypto Trading Web Control Plane

This binary is the trusted composition root for the local read-only HTTP/SSE and Web adapters.
It accepts one bounded execution journal, a durable generation ID, and a loopback port. It has no
live-trading flag and exposes no command route.

From the `rust/` workspace:

```powershell
cargo +1.85.0 run -p crypto-trading-web-app --bin crypto-trading-web -- `
  --history-path fixtures/m2-operator-journal.jsonl `
  --journal-id 44444444-4444-4444-8444-444444444444 `
  --port 8787
```

Then open `http://127.0.0.1:8787/overview`.

For optional bearer authentication, put a 32–512 byte token in an uppercase environment variable
and pass only its name:

```powershell
$env:CRYPTO_TRADING_WEB_TOKEN = '<generated secret>'
cargo +1.85.0 run -p crypto-trading-web-app --bin crypto-trading-web -- `
  --history-path fixtures/m2-operator-journal.jsonl `
  --journal-id 44444444-4444-4444-8444-444444444444 `
  --bearer-token-env CRYPTO_TRADING_WEB_TOKEN
```

The shell remains data-free and same-origin. The browser keeps the supplied token only in memory
and uses authenticated `fetch` requests for JSON and event-stream reads.
