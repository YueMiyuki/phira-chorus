# chorus

A multiplayer server for [Phira](https://github.com/TeamFlos/phira), built to
host massive amount of players on one machine.

| Crate | Purpose |
|---|---|
| `chorus-server` | The server binary (and a library for embedding/tests) |
| `chorus-proto` | Wire protocol types and codec |
| `chorus-client` | Minimal async client connection |
| `chorus-bench` | Load generator and a stand-in for the Phira API |

## Running

Prebuilt binaries are attached to each release: static Linux builds (x86_64
and arm64, any distribution), macOS and Windows. Pushing a `v*` tag builds
and publishes them ([`release.yml`](.github/workflows/release.yml)).

To build from source (Rust 1.88 or later):

```shell
cargo build --release -p chorus-server
RUST_LOG=info ./target/release/chorus-server --port 12346
```

The server keeps remembered logins in `logins.json` in its working directory
(see `login_store`).

| Flag | Default | |
|---|---|---|
| `-p, --port` | `12346` | Port to listen on |
| `--bind` | `::` | Address to bind; `::` accepts IPv6 and IPv4 |
| `-c, --config` | `server_config.yml` | Config file; a missing file means defaults |
| `--workers` | CPU cores | Runtime worker threads |
| `--log-dir` | none | Also write hourly-rotated log files here |

Logging defaults to `info`; override with `RUST_LOG` (e.g. `RUST_LOG=debug`,
or `RUST_LOG=chorus_server=debug,info`).

## Configuration

All keys are optional. See [`server_config.example.yml`](server_config.example.yml).

| Key | Default | |
|---|---|---|
| `monitors` | `[2]` | User ids allowed to join rooms as monitors |
| `api_url` | `https://phira.5wyxi.com` | Phira API used for logins, charts and records |
| `room_max_users` | `8` | Player slots per room (monitors excluded) |
| `max_connections` | `30000` | Further logins are told the server is full |
| `reconnect_grace_secs` | `10` | How long a disconnected player keeps their seat outside of a game |
| `playing_grace_secs` | `10` | How long a player who drops mid-game can reconnect into it; `0` removes them at once |
| `login_timeout_secs` | `30` | Connections must send their login within this time (a login waiting on Phira is never cut off) |
| `api_wait_secs` | `6` | How long chart selection and result uploads wait for Phira before the player is answered; keep under the client's 7 s |
| `api_retry_secs` | `60` | How long one lookup retries a slow or failing Phira before starting over |
| `api_concurrency` | `128` | Requests to Phira in flight at once, for logins and for everything else |
| `chart_cache_secs` / `chart_cache_size` | `600` / `4096` | Chart metadata cache |
| `login_cache_secs` | `60` | Logins Phira confirmed this recently are reused without asking it again |
| `login_remember_days` | `30` | Players who logged in within this time get in at once, even while Phira is slow or down; `0` always waits for Phira |
| `login_cache_size` | `100000` | Remembered logins kept |
| `login_store` | `logins.json` | File remembered logins are saved to, so they survive restarts; empty disables |
| `outbox_lossy_limit` | `262144` | Queued bytes past which relayed live data is dropped for a slow connection |
| `outbox_limit` | `4194304` | Queued bytes past which a connection is closed |
| `command_rate` / `command_burst` | `100` / `400` | Per-connection request rate limit (pings excluded) |
| `stats_interval_secs` | `60` | Period of the `stats` log line; `0` disables |
