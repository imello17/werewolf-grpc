#!/usr/bin/env python3
"""
stress_idle_streams.py — reproduces the classmate-reported starvation bug
against YOUR OWN server, for the before/after comparison in the writeup.

It opens N Play() streams (each on its own TCP connection) that never send
the required `join`, then checks whether a real player can still join, and
samples the server's OS thread count from /proc.

Against the Project 1 server (thread-per-stream, no join timeout) enough idle
streams exhaust the thread pool and the probe player never gets in. Against
the Project 2 server (asyncio, join timeout) the probe joins immediately and
the idle streams are closed by the server after the timeout.

Usage:
    python3 stress_idle_streams.py --streams 40 --server-pid $(pgrep -f '^python3 server.py')
Only point this at a server you are running yourself.
"""
import argparse
import asyncio
import time

import grpc
import werewolf_pb2 as pb
import werewolf_pb2_grpc as pb_grpc


def threads_of(pid):
    if not pid:
        return None
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("Threads:"):
                    return int(line.split()[1])
    except OSError:
        return None


async def idle_stream(addr, results):
    """Open a stream and never send join; record how it ends."""
    ch = grpc.aio.insecure_channel(addr, options=[
        ("grpc.use_local_subchannel_pool", 1)])   # separate TCP connection
    call = pb_grpc.WerewolfStub(ch).Play()
    try:
        await call.read()                          # wait to be kicked
        results.append("EOF")
    except grpc.aio.AioRpcError as e:
        results.append(e.code().name)
    except asyncio.CancelledError:
        results.append("still-open")
        raise
    finally:
        await ch.close()


async def probe(addr, name, timeout):
    """A real player: send join, wait for the first server message."""
    ch = grpc.aio.insecure_channel(addr, options=[
        ("grpc.use_local_subchannel_pool", 1)])
    call = pb_grpc.WerewolfStub(ch).Play()
    t0 = time.monotonic()
    try:
        await call.write(pb.ClientMsg(join=pb.JoinMsg(name=name)))
        msg = await asyncio.wait_for(call.read(), timeout)
        ok = msg is not grpc.aio.EOF
        detail = msg.text if ok else "EOF"
    except asyncio.TimeoutError:
        ok, detail = False, f"no reply within {timeout}s"
    except grpc.aio.AioRpcError as e:
        ok, detail = False, f"{e.code().name}: {e.details()}"
    elapsed = time.monotonic() - t0
    call.cancel()
    await ch.close()
    return ok, elapsed, detail


async def main(a):
    addr = f"{a.host}:{a.port}"
    print(f"server threads before: {threads_of(a.server_pid)}")
    results = []
    tasks = [asyncio.create_task(idle_stream(addr, results))
             for _ in range(a.streams)]
    await asyncio.sleep(2)
    print(f"opened {a.streams} idle streams; "
          f"server threads now: {threads_of(a.server_pid)}")

    ok, dt, detail = await probe(addr, a.name, a.probe_timeout)
    print(f"real player join: {'OK' if ok else 'FAILED'} in {dt:.2f}s -> {detail}")

    await asyncio.sleep(a.hold)
    print(f"after {a.hold}s: server threads: {threads_of(a.server_pid)}")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    counts = {}
    for r in results:
        counts[r] = counts.get(r, 0) + 1
    print(f"idle streams ended as: {counts}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--streams", type=int, default=40)
    ap.add_argument("--server-pid", type=int, default=None)
    ap.add_argument("--name", default="probe")
    ap.add_argument("--probe-timeout", type=float, default=5.0)
    ap.add_argument("--hold", type=float, default=12.0,
                    help="seconds to keep idle streams open after the probe")
    asyncio.run(main(ap.parse_args()))
