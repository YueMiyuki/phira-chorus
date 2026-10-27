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
| `-p, --port` | `port` from the config | Port to listen on |
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
| `port` | `12346` | Port of the game server; `--port` overrides it |
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
| `stats_interval_secs` | `60` | Period of the `stats` log line, skipped while it would read the same as the last one; `0` disables |
| `http_addr` | `""` | Address of the [HTTP API](#http-api), on its own port apart from the game port. `":12347"` is that port on the `--bind` address (IPv6 and IPv4 by default); empty disables |
| `http_tokens` | `{}` | Bearer tokens the HTTP API accepts, by name; no tokens refuses every request |

## HTTP API

With `http_addr` set, the server also serves an HTTP API on that address,
separate from the game port. `GET /api` returns a plain-text guide to it,
with no token needed. Every other route is under `/v1`: `GET /v1/rooms` lists
the rooms and `GET /v1/status` reports players online and CPU and memory use.
Those routes need one of the `http_tokens` as `Authorization: Bearer <token>`;
each request is logged with the name of its token.

```yaml
http_addr: ":12347"
http_tokens:
  dashboard: some-long-random-string
```

```shell
curl -H "Authorization: Bearer some-long-random-string" http://localhost:12347/v1/rooms
```

```json
{
  "rooms": [
    {
      "roomid": "112233",
      "cycle": false,
      "lock": true,
      "host": { "name": "泪稚苒", "id": "3461054" },
      "state": "playing",
      "chart": { "name": "星降る夜と一輪の花", "id": "69901" },
      "players": [
        { "name": "泪稚苒", "id": 3461054 },
        { "name": "聆碎a", "id": 3462386 }
      ]
    }
  ]
}
```

`state` is `select_chart`, `waiting_for_ready` or `playing`; `chart` is `null`
until one is selected. `players` leaves out monitors.

```json
{
  "status": "ok",
  "players": 128,
  "cpu": { "process": 3.5, "system": 12.5 },
  "ram": { "process": 52428800, "used": 6442450944, "total": 17179869184 }
}
```

In `/v1/status`, `players` counts everyone logged in, monitors included. `cpu`
is the percent of the whole machine used by the server and by everything,
averaged over 5 seconds; `ram` is in bytes.
