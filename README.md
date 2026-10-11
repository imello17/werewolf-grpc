# Werewolf over gRPC

A text-based Werewolf (Mafia) game. Each player is a separate OS process (and a
separate Linux user) that talks to a moderator server over **gRPC bidirectional
streaming on HTTP/2 (TCP)** — a communications primitive more sophisticated than
named pipes. The game is network-capable: clients can connect from other hosts.

## The communications library

**File: `comms.py`** — this is the single-file communications library. It is a
thin wrapper over gRPC's Python bindings; `server.py` and `client.py` import
only `comms.py` and never reference a gRPC symbol directly.

### API

Server:

| Call | Pattern | Meaning |
|------|---------|---------|
| `CommsServer(host, port)` | — | create the moderator endpoint |
| `srv.on_join / on_chat / on_vote / on_leave` | — | game-logic callbacks |
| `srv.start()` | — | start the gRPC server (non-blocking) |
| `srv.send(pid, text, ...)` | **unicast** | one player's stream |
| `srv.multicast(pids, text, ...)` | **multicast** | a subset (e.g. wolves) |
| `srv.broadcast(text, ...)` | **broadcast** | every connected player |
| `srv.names()` | — | `{player_id: name}` of connected players |
| `srv.disconnect(pid)` / `srv.stop()` | — | close a stream / shut down |

Client:

| Call | Meaning |
|------|---------|
| `CommsClient(host, port, name)` | create a player endpoint |
| `cli.on_event = fn(ServerMsg)` | callback for every server message |
| `cli.connect()` | open the `Play()` stream, send join, start reader |
| `cli.chat(text)` / `cli.vote(target)` | send actions upstream |
| `cli.close()` | leave |

All traffic rides **one** RPC: `Play(stream ClientMsg) returns (stream ServerMsg)`.
A player opens it once; actions go up and every game event comes down on that same
long-lived stream. See `werewolf.proto`.

## Files

- `werewolf.proto` — service + message definitions (the wire contract)
- `comms.py` — **the communications library** (wraps gRPC)
- `server.py` — the moderator: phase FSM + routing; game loop on its own thread
- `client.py` — a player terminal
- `werewolf_pb2.py`, `werewolf_pb2_grpc.py` — generated from the `.proto` (see below)

## Setup

```bash
python3 -m pip install grpcio grpcio-tools
# generate the stubs from the proto:
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. werewolf.proto
```

## Play (local, 3+ terminals)

```bash
# terminal 1 — moderator
python3 server.py --players 3

# terminals 2..N — players
python3 client.py --name alice
python3 client.py --name bob
python3 client.py --name carol
```

Per the course requirement that each player be its own Linux user, run each
client as a different user, e.g. `sudo -iu player0 python3 client.py --name alice`.

While playing: type to chat; `/vote NAME` during a voting phase; `/quit` to leave.

## Distributed (network) play

Start the server bound to all interfaces (`--host 0.0.0.0`, the default) and point
clients at its address: `python3 client.py --host <server-ip> --name alice`.

## Known weaknesses (intentional, for security review)

- **No authentication**: identity is the server-assigned `player_id` bound to a
  stream. Anyone who can reach the port can join.
- **No TLS**: `add_insecure_port` / `insecure_channel` — traffic is plaintext and
  can be sniffed (e.g. the wolves' night chat) with `tcpdump`.

Both are left in deliberately so classmates have concrete things to critique.
