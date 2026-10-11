#!/usr/bin/env python3
"""
client.py — a player. Opens one bidi stream via the comms library, prints every
server event, and sends whatever you type as chat (or `/vote NAME` as a vote).

It NEVER touches gRPC: all I/O is through comms.CommsClient.

Usage:  python3 client.py --name alice [--host 127.0.0.1] [--port 50051]
Commands while playing:
    /vote <name>   cast a vote for <name> during a voting phase
    anything else  sent as chat (relayed only during phases you may speak in)
    /quit          leave
"""
import argparse
import sys
import threading

import comms
import werewolf_pb2 as pb

_SCOPE = {pb.ServerMsg.ALL: "ALL",
          pb.ServerMsg.WOLVES: "WOLVES",
          pb.ServerMsg.PRIVATE: "PRIVATE"}


def render(ev):
    scope = _SCOPE.get(ev.scope, "?")
    if ev.type == pb.ServerMsg.CHAT:
        print(f"[{scope}] {ev.sender}: {ev.text}")
    elif ev.type == pb.ServerMsg.ROLE:
        print(f"\n*** {ev.text} ***\n")
    elif ev.type == pb.ServerMsg.PHASE:
        print(f"\n--- {ev.text} ---")
    elif ev.type in (pb.ServerMsg.PROMPT, pb.ServerMsg.DEATH,
                     pb.ServerMsg.GAMEOVER):
        print(f">> {ev.text}")
    else:
        print(ev.text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    args = ap.parse_args()

    cli = comms.CommsClient(host=args.host, port=args.port, name=args.name)
    done = threading.Event()

    def on_event(ev):
        render(ev)
        if ev.type == pb.ServerMsg.GAMEOVER:
            done.set()

    cli.on_event = on_event
    cli.connect()
    print(f"[connected as {args.name}]  type to chat, '/vote NAME' to vote, '/quit' to leave")

    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            if line == "/quit":
                break
            if line.startswith("/vote "):
                cli.vote(line[len("/vote "):].strip())
            else:
                cli.chat(line)
            if done.is_set():
                break
    except KeyboardInterrupt:
        pass
    finally:
        cli.close()


if __name__ == "__main__":
    main()
