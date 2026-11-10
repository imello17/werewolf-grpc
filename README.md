# Werewolf over async gRPC

A text-based Werewolf (Mafia) game. Each player is a separate OS process (and a
separate Linux user) that talks to a moderator server over **gRPC bidirectional
streaming on HTTP/2 (TCP)**, one long-lived stream per player.

**Project 2 change:** the comms library moved from the synchronous gRPC server
(one OS thread per player stream, from a fixed thread pool) to **asynchronous
I/O with `grpc.aio` + `asyncio`** (one event loop, one coroutine per stream).
This removes the thread-pool-exhaustion bug a classmate found in Project 1, and
adds join timeouts, unique validated names, and disconnect handling. The game
also gains two roles: a **doctor** and a **jester**, and optional **mutual TLS**
(per-player certificates) that encrypts all traffic and ties each in-game name
to a Linux user.

## The communications library

**File: `comms.py`**: the single-file communications library. It wraps gRPC's
Python bindings; `server.py` and `client.py` import only `comms.py` and never
reference a gRPC symbol. All calls must be made from the asyncio event loop.

### API

Server:

| Call | Pattern | Meaning |
|------|---------|---------|
| `CommsServer(host, port, max_players, join_timeout, tls=None, ...)` | | create the moderator endpoint |
| `srv.on_join(pid, name)` | | callback; return a string to reject the player |
| `srv.on_chat / on_vote / on_action / on_leave` | | game-logic callbacks |
| `await srv.start()` / `await srv.stop()` | | start / shut down the server |
| `srv.send(pid, text, ...)` | **unicast** | one player's stream |
| `srv.multicast(pids, text, scope=WOLVES\|GROUP, ...)` | **multicast** | a subset (wolves, or the jester's voters) |
| `srv.broadcast(text, ...)` | **broadcast** | every connected player |
| `srv.names()` | | `{player_id: name}` of connected players |
| `srv.disconnect(pid)` | | flush queued messages, then close that stream |

Client:

| Call | Meaning |
|------|---------|
| `CommsClient(host, port, name, tls=None)` | create a player endpoint (with TLS, `name` may be empty) |
| `TLSConfig(ca, cert, key)` | paths to PEM files; pass as `tls=` on either side |
| `cli.on_event = fn(ServerMsg)` | callback for every server message |
| `cli.on_close = fn(reason)` | callback when the stream ends (with the server's reason) |
| `await cli.connect()` | open the `Play()` stream and send the join |
| `cli.chat(text)` / `cli.vote(target)` / `cli.act(action, target)` | send actions upstream |
| `await cli.close()` | leave |

All traffic rides **one** RPC: `Play(stream ClientMsg) returns (stream ServerMsg)`.
See `werewolf.proto`.

## Files

- `werewolf.proto`: service and message definitions (the wire contract)
- `comms.py`: **the communications library** (async gRPC)
- `server.py`: the moderator: game loop coroutine, roles, night/day phases
- `client.py`: a player terminal
- `make_certs.sh`: creates the game CA, the server certificate and per-player certificates
- `test/play_game.sh`: plays a scripted 7-player mTLS game in real `client.py` terminals inside tmux (for screenshots)
- `test/bot_player.py`, `test/run_demo.sh`: headless 7-player mTLS game with bots (smoke test)
- `stress_idle_streams.py`: reproduces the Project 1 starvation bug for testing (point it only at your own server)
- `werewolf_pb2.py`, `werewolf_pb2_grpc.py`: generated from the `.proto` (see below)

## Setup

```bash
python3 -m pip install grpcio grpcio-tools
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. werewolf.proto
```

## Play (plaintext, quick)

```bash
python3 server.py --players 7                  # terminal 1
python3 client.py --name alice                 # terminals 2..N
```

## Play with mutual TLS (one Linux user per player)

Run once as root, from the game directory (e.g. `/opt/werewolf`):

```bash
./make_certs.sh ca                             # certs/ca.key stays private
./make_certs.sh server                         # add server IPs/hostnames for remote play
for i in 0 1 2 3 4 5 6; do id player$i >/dev/null 2>&1 || useradd -M -s /bin/bash player$i; done
./make_certs.sh player ali player0             # key owned by player0, mode 600
./make_certs.sh player bob player1
# ...one per player
chmod 755 certs && chmod -R a+rX . && chmod 600 certs/*.key
```

Then:

```bash
python3 server.py --players 7 --tls certs                       # moderator
sudo -iu player0 python3 /opt/werewolf/client.py --tls /opt/werewolf/certs --cert-name ali
sudo -iu player1 python3 /opt/werewolf/client.py --tls /opt/werewolf/certs --cert-name bob
...
```

Your in-game name is your certificate's CN. `--name`, if given, must match it.
Remote play: create the server cert with its address (`./make_certs.sh server 192.168.1.20`)
and copy `ca.crt` plus each player's own `.crt`/`.key` to their machine.

Commands: type to chat; `/vote NAME`; `/save NAME` (doctor); `/revenge NAME`
(jester's ghost); `/quit`.

Useful server flags: `--fast` (all timers / 5), `--seed N` (reproducible roles),
`--doctor-min N` / `--jester-min N` (player count needed for each role; 0 means
always), `--join-timeout S`, `--max-players N`, `--tls DIR`.

Automated demo (after the setup above): `sudo test/run_demo.sh`.

## Rules

- **Roles:** about 1 wolf per 3 players. A **doctor** appears at 5+ players and a
  **jester** at 7+ (both configurable). Wolves are told who their packmates are.
- **Night:** wolves chat privately, then vote on a victim. At the same time the
  doctor may `/save` someone (self-saves allowed, but not the same person two
  nights in a row). A wolf kill on the saved player is prevented, and the doctor
  is told "You saved a life tonight."
- **Day:** open chat, then everyone votes. A vote window closes early once
  everyone eligible has voted. Ties mean no lynching.
- **Jester:** wins (individually) if the town lynches him. The players who voted
  for him are warned by a group multicast. The next night his ghost `/revenge`s
  one of them; if he doesn't choose in time, one is picked at random. The doctor
  cannot block this.
- **Win:** TOWN when no wolves remain; WOLVES when wolves >= town-side
  (townspeople + doctor). The jester counts for neither side.
- **Disconnects:** a player who drops out is removed from the game, their votes
  are discarded, and their role is revealed.

## Distributed (network) play

The server binds to all interfaces by default; point clients at it with
`python3 client.py --host <server-ip> --name alice`.

## Robustness limits (in `comms.py`)

- **Join timeout:** a stream must send `join` within 10s or it is aborted.
- **Names:** must be unique (case-insensitive) and 1-16 characters of `[A-Za-z0-9_-]`.
- **Message size:** messages over 4 KB are rejected by gRPC.
- **Chat rate limit:** at most one chat line per 0.25s per player.
- **Outbound queue:** bounded per player; a client that falls too far behind is disconnected.
- **Dead peers:** HTTP/2 keepalive pings detect dead TCP connections.

## Security with `--tls`

| Attack | Plaintext mode | mTLS mode |
|---|---|---|
| Sniff the wolves' night chat (`tcpdump -i lo -A port 50051`) | chat readable | ciphertext only |
| Connect with no certificate | joins | TLS handshake fails |
| Certificate from a different CA | n/a | TLS handshake fails |
| Valid cert for `eve`, join claiming `bob` | joins as bob | `PERMISSION_DENIED` |
| Linux user `player1` uses `ali`'s key | n/a | can't read the key file (mode 600) |
| Flood of idle streams without a certificate | ties up pre-join slots | rejected before `Play()` runs |

Failed handshakes appear in the server log as gRPC `Handshake failed` lines.

## Known weaknesses

- **Plaintext mode** (no `--tls`) is still unauthenticated and unencrypted, kept for comparison.
- **No revocation:** a leaked player key stays valid until its certificate expires (365 days).
- **The CA key is all-powerful:** whoever holds `certs/ca.key` can mint a certificate for any name.
- **Insiders:** a player with a valid certificate can still open many streams up to the
  server's limits, though every one of them is attributable to their name.
