# replicator-rs

[![Build Status](https://github.com/hallowslab/replicator-rs/actions/workflows/rust.yml/badge.svg)](https://github.com/hallowslab/replicator-rs/actions/workflows/rust.yml)

Durable replication worker for ExifFlow. It reads the SQLite record store written
by `rftps` and `recv` (`--record-store`) and replays each recorded upload to
remote storage. It is the only component that talks to the Access Broker or to a
remote target; the upload servers never do.

It ships two ways:

* **Standalone CLI** - its own binary, run with `--store` and `--config`.
* **Library** - `replicator_rs::Consumer` and the broker/storage clients are
  embedded as a thread in the ExifFlow desktop app.

## How it fits

```
rftps / recv  --(record)-->  record store (SQLite, WAL)  <--(drain)-->  replicator-rs  -->  remote storage
```

- Upload servers append a `pending` row per upload. They never hold broker or
  storage credentials.
- `replicator-rs` claims rows, uploads, and marks them `done`. It is resumable and
  retries with backoff.
- The store schema is a contract shared with `rftps` and `recv`; keep it in sync.

## Features

- **Durable queue** - SQLite (WAL), atomic `pending` / `in-flight` / `done`
  transitions, crash-safe resume, retry backoff, 72h dead-letter, `reset_failed`
  on restart.
- **Broker mode** - zero-trust device registration and approval against the
  ExifFlow Access Broker; storage credentials are fetched at runtime and held in
  memory only.
- **Static storage mode** - skip the broker and point at an S3 or FTPS target
  directly in `bg.json`.
- **Resumable multipart S3** - persists `upload_id` and completed parts, so a
  failed upload resumes instead of restarting from zero.
- **Pluggable features** - `broker` and `s3` are default-on. Build with
  `--no-default-features` for a static FTPS-only worker.

## Quick start

### Broker mode

```bash
# 1. Create a config (interactive; writes bg.json with a fresh device key)
replicator-rs init

# 2. Run against the store written by rftps/recv
replicator-rs --store /var/lib/exifflow/records.db --config bg.json
```

On the first upload the device registers with the broker and waits for approval
in the broker dashboard; subsequent uploads replicate automatically.

### Static storage mode (no broker)

Write `bg.json` with a `storage` section and no `broker` section; replication
starts immediately.

```json
{
  "enabled": true,
  "max_parallel_jobs": 2,
  "queue_capacity": 1000,
  "storage": {
    "version": 1,
    "backend": {
      "type": "s3",
      "endpoint": "https://s3.example.com",
      "region": "us-east-1",
      "bucket": "exifflow",
      "path_style": true,
      "access_key_id": "AKIA...",
      "secret_access_key": "...",
      "session_token": null,
      "path_prefix": "uploads",
      "ca_cert_pem": null,
      "multipart_threshold_bytes": null,
      "max_part_size_bytes": null,
      "immutable_naming": false
    }
  }
}
```

FTPS target instead of S3:

```json
{
  "enabled": true,
  "storage": {
    "version": 1,
    "backend": {
      "type": "ftps",
      "host": "backup.example.com",
      "port": 990,
      "username": "replicator",
      "password": "...",
      "path_prefix": "/exifflow",
      "use_ssl": true,
      "ca_cert": null,
      "ca_cert_pem": null,
      "danger_disable_cert_verify": false
    }
  }
}
```

Do not set both `broker` and `storage` in the same file; if `broker` is present it
wins.

## CLI

| Command / flag | Description |
| -------------- | ----------- |
| `replicator-rs init` | interactive `bg.json` generator (broker device key + URL) |
| `replicator-rs keygen` | print a raw Ed25519 device key |
| `--store <path>` | durable store written by `rftps`/`recv` `--record-store` |
| `--config <path>` | `bg.json` replication config |
| `--multipart-threshold <bytes>` | override the baked multipart threshold |
| `--max-part-size <bytes>` | override the baked per-part ceiling |

The `init` and `keygen` subcommands are only available with the `broker` feature
(default on).

## Broker config shape (`bg.json`)

```json
{
  "enabled": true,
  "broker": {
    "url": "http://127.0.0.1:8700",
    "device_key": "<64-hex-chars>",
    "device_name": "test-device",
    "approval_timeout_secs": 1800,
    "ca_cert": null,
    "ca_cert_pem": null,
    "danger_disable_cert_verify": false,
    "broker_messages": true,
    "immutable_naming": true
  }
}
```

- `device_key` is the Ed25519 signing identity. Keep it stable across restarts
  (otherwise the device re-registers and needs re-approval).
- `immutable_naming = true` prepends a UUID to the remote key so same-name
  uploads never overwrite each other remotely.
- Self-signed broker or storage TLS: pass the CA via `ca_cert`/`ca_cert_pem`
  (verification is never disabled; the CA becomes a trusted root). Use
  `danger_disable_cert_verify` only for local testing.

## Build-time defaults

`build.rs` reads `build.toml` and bakes S3 multipart defaults into the binary
(`multipart_threshold_bytes`, `max_part_size_bytes`). Runtime `bg.json` values and
the CLI flags override them. See `build.toml.example`.

## Build

```bash
cargo build --release                      # default: broker + s3
cargo build --release --no-default-features # static FTPS only
cargo test
```

Inside the ExifFlow workspace, build just this crate with
`cargo build --release -p replicator-rs`.

## Releases

Tagged releases (`v*.*.*`) build `replicator-rs-full` (broker + s3) and
`replicator-rs-ftps` (`--no-default-features`) for Linux, macOS (x64 and arm64),
and Windows, then publish them with the notes in `docs/releases/<tag>.md`. See
[`.github/workflows/rust.yml`](.github/workflows/rust.yml).
