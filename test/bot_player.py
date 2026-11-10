#!/usr/bin/env python3
"""Scripted player for the automated demo run. One process per player,
run as that player's Linux user, using only that user's certificate.
Prints a client.py-style transcript to stdout."""
import argparse, asyncio, sys
sys.path.insert(0, "/opt/werewolf")
import comms, client, werewolf_pb2 as pb

ap = argparse.ArgumentParser()
ap.add_argument("--me", required=True)
ap.add_argument("--port", type=int, default=50051)
ap.add_argument("--certs", default="/opt/werewolf/certs")
a = ap.parse_args()

role, pack, alive = None, [], set()
last_saved = [None]

def first_alive(exclude=()):
    c = sorted(n for n in alive if n not in exclude)
    return c[0] if c else None

async def main():
    tls = comms.TLSConfig(f"{a.certs}/ca.crt", f"{a.certs}/{a.me}.crt",
                          f"{a.certs}/{a.me}.key")
    cli = comms.CommsClient(port=a.port, tls=tls)       # name comes from cert
    done = asyncio.Event()
    loop = asyncio.get_running_loop()

    def later(fn):                                       # act like a human, a bit later
        loop.call_later(0.4, fn)

    def on_event(ev):
        global role, pack
        client.render(ev)
        t = ev.text
        if ev.type == pb.ServerMsg.ROLE:
            role = t.split("You are ")[1].split(".")[0].split()[-1].lower()
        if t.startswith("Your pack: "):
            pack = t[len("Your pack: "):].split(", ")
        if "Alive: " in t or "Valid: " in t:
            alive.clear(); alive.update(t.split(": ")[-1].split(", "))
        if ev.type == pb.ServerMsg.DEATH and "They were" in t:
            alive.discard(t.split()[0])
        if ev.type == pb.ServerMsg.GAMEOVER:
            done.set()
        if ev.type != pb.ServerMsg.PROMPT:
            return
        if "Wolves, discuss" in t:
            alive.clear(); alive.update(t.split("Targets: ")[1].split(", "))
            later(lambda: cli.chat(f"let's take {first_alive(pack)} tonight"))
        elif "Wolves, /vote" in t:
            later(lambda: cli.vote(first_alive(pack)))
        elif "Doctor, /save" in t:
            alive.clear(); alive.update(t.split("Alive: ")[1].split(", "))
            pick = first_alive([last_saved[0]] if last_saved[0] else [])
            last_saved[0] = pick
            later(lambda: cli.act("save", pick))
        elif "ghost rises" in t:
            pick = sorted(t.split("Choose from: ")[1].split(", "))[-1]
            later(lambda: cli.act("revenge", pick))
        elif "Discuss" in t:
            later(lambda: cli.chat(f"I'm {a.me}, and I'm definitely innocent"))
        elif "lynch someone" in t:
            c = sorted(n for n in alive if n != a.me and n not in pack)
            later(lambda: cli.vote(c[-1]))

    cli.on_event = on_event
    cli.on_close = lambda r: (print(f"[disconnected: {r}]"), done.set())
    await cli.connect()
    print(f"[connected over mTLS as cert {a.me}]")
    try:
        await asyncio.wait_for(done.wait(), 120)
    finally:
        await cli.close()

asyncio.run(main())
