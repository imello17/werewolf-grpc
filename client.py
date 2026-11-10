#!/usr/bin/env python3
"""
client.py — a player. Opens one bidi stream via the comms library, prints every
server event, and sends what you type.

It NEVER touches gRPC: all I/O is through comms.CommsClient. The client runs
on asyncio; a small daemon thread feeds stdin lines into the event loop
(blocking terminal input is the one thing asyncio can't await portably).

Usage:  python3 client.py --name alice [--host 127.0.0.1] [--port 50051]
        python3 client.py --tls certs/ --cert-name alice      # mutual TLS
  With --tls the in-game name comes from your certificate's CN; --name is
  optional and, if given, must match it.
Commands while playing:
    /vote NAME      vote during a voting phase
    /save NAME      (doctor) protect someone tonight
    /revenge NAME   (jester's ghost) take one of the players who voted for you
    /quit           leave
    anything else   chat (relayed only in phases where you may speak)
"""
import argparse
import asyncio
import sys
import threading

import comms
import werewolf_pb2 as pb

_SCOPE = {pb.ServerMsg.ALL: "ALL", pb.ServerMsg.WOLVES: "WOLVES",
          pb.ServerMsg.PRIVATE: "PRIVATE", pb.ServerMsg.GROUP: "GROUP"}


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
    elif scope in ("WOLVES", "GROUP"):
        print(f"[{scope}] {ev.text}")
    else:
        print(ev.text)


async def amain(args):
    tls = None
    if args.tls:
        d, who = args.tls.rstrip("/"), args.cert_name or args.name
        if not who:
            raise SystemExit("--tls needs --cert-name (or --name)")
        tls = comms.TLSConfig(ca=f"{d}/ca.crt", cert=f"{d}/{who}.crt",
                              key=f"{d}/{who}.key")
        try:
            tls.read()
        except PermissionError as e:
            raise SystemExit(f"cannot read {e.filename}: that certificate "
                             "belongs to another Linux user")
        except FileNotFoundError as e:
            raise SystemExit(f"missing {e.filename} (see make_certs.sh)")
    elif not args.name:
        raise SystemExit("--name is required without --tls")
    cli = comms.CommsClient(host=args.host, port=args.port,
                            name=args.name or "", tls=tls)
    done = asyncio.Event()
    state = {"gameover": False}

    def on_event(ev):
        render(ev)
        if ev.type == pb.ServerMsg.GAMEOVER:
            state["gameover"] = True
            done.set()

    def on_close(reason):
        if not state["gameover"]:
            print(f"[disconnected: {reason}]")
        done.set()

    cli.on_event = on_event
    cli.on_close = on_close
    await cli.connect()
    who = args.name or args.cert_name
    print(f"[connecting as {who}{' over mTLS' if tls else ''}]  chat by typing; /vote, /save, "
          f"/revenge NAME; /quit to leave")

    loop = asyncio.get_running_loop()
    lines = asyncio.Queue()

    def stdin_pump():
        try:
            for line in sys.stdin:
                loop.call_soon_threadsafe(lines.put_nowait, line)
            loop.call_soon_threadsafe(lines.put_nowait, None)   # EOF
        except RuntimeError:
            pass                                                 # loop closed

    threading.Thread(target=stdin_pump, daemon=True).start()

    done_task = asyncio.create_task(done.wait())
    try:
        while True:
            get_task = asyncio.create_task(lines.get())
            finished, _ = await asyncio.wait(
                {get_task, done_task}, return_when=asyncio.FIRST_COMPLETED)
            if done_task in finished:
                get_task.cancel()
                break
            line = get_task.result()
            if line is None:
                break
            line = line.strip()
            if not line:
                continue
            cmd, _, arg = line.partition(" ")
            if cmd == "/quit":
                break
            elif cmd == "/vote":
                cli.vote(arg.strip())
            elif cmd == "/save":
                cli.act("save", arg.strip())
            elif cmd == "/revenge":
                cli.act("revenge", arg.strip())
            else:
                cli.chat(line)
    finally:
        done_task.cancel()
        await cli.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default=None)
    ap.add_argument("--tls", metavar="CERT_DIR", default=None,
                    help="use mutual TLS; dir holds ca.crt and NAME.crt/.key")
    ap.add_argument("--cert-name", default=None,
                    help="which NAME.crt/NAME.key to use (default: --name)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    args = ap.parse_args()
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
