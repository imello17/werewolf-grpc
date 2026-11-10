"""
comms.py  —  the single-file communications library for Werewolf-over-gRPC.

THIS IS THE COMMUNICATIONS LIBRARY the assignment asks for. Everything that
touches gRPC lives here; server.py and client.py import only this file and
never see a gRPC symbol. It is a thin wrapper over gRPC's Python bindings.

----------------------------------------------------------------------------
PRIMITIVE (Project 2):  gRPC bidirectional streaming over HTTP/2 (TCP),
                        driven by ASYNCHRONOUS I/O (grpc.aio + asyncio).

Project 1 used the synchronous grpc server: every Play() stream occupied one
OS thread from a fixed ThreadPoolExecutor for its whole lifetime, and the
handler blocked on the first message with no timeout. A classmate showed
that ~40 connections that never sent `join` pinned every pool thread and
locked real players out. Project 2 replaces the threads with coroutines:

    * One event loop, one OS thread. Each Play() stream is a coroutine that
      costs a few KB, not a thread. An idle stream parked in `await` costs no
      thread at all, so idle connections can no longer starve the server.
    * Because everything runs on one thread, game state needs NO LOCKS:
      callbacks and the game loop never run truly in parallel; they
      interleave only at `await` points.
    * Every wait is bounded: the first message must arrive within
      `join_timeout` seconds (asyncio.wait_for) or the stream is aborted.

COMMS PATTERNS (the rubric's "non-trivial comms"):
    send(pid, ...)             -> UNICAST   (one HTTP/2 stream)
    multicast(pids, ...)       -> MULTICAST (a subset of streams: wolves, or
                                             an ad-hoc GROUP like the
                                             jester's voters)
    broadcast(...)             -> BROADCAST (every connected stream)

CONCURRENCY model per stream:
    reader task : awaits context.read() and dispatches to on_chat/on_vote/...
    writer task : awaits the player's bounded asyncio.Queue and context.write()s
    The handler waits on both; when either ends (client left, server closed
    the player, or the client was too slow to keep up) the other is cancelled,
    the player is removed and on_leave fires. That last step is what lets the
    game notice disconnects (P1 kept dead players "alive").

ROBUSTNESS limits (all constructor args):
    join_timeout      seconds allowed before the first message must be `join`
    max_pending       streams allowed to sit in the pre-join state at once
    max_players       joined players allowed at once
    max_msg_bytes     largest ClientMsg accepted (enforced by gRPC itself)
    outbound_max      per-player outbound queue bound; a client that falls
                      this far behind is disconnected instead of letting its
                      queue grow without limit
    chat_min_interval per-player chat rate limit (extra lines are dropped)

----------------------------------------------------------------------------
PUBLIC API   (all calls must be made from the asyncio event loop)
----------------------------------------------------------------------------
Server side:
    srv = CommsServer(host, port, ...)
    srv.on_join   = fn(pid, name) -> None | str   # return a str to reject
    srv.on_chat   = fn(pid, text)
    srv.on_vote   = fn(pid, target)
    srv.on_action = fn(pid, action, target)
    srv.on_leave  = fn(pid)
    await srv.start()
    srv.send(pid, text, mtype=, scope=, phase=, sender=)          # UNICAST
    srv.multicast(pids, text, mtype=, scope=, phase=, sender=)    # MULTICAST
    srv.broadcast(text, mtype=, scope=, phase=, sender=)          # BROADCAST
    srv.names()            # {pid: name} of connected players
    srv.disconnect(pid)    # flush pending messages, then close that stream
    await srv.stop()

Client side:
    cli = CommsClient(host, port, name)
    cli.on_event = fn(ServerMsg)        # every server message
    cli.on_close = fn(reason: str)      # stream ended (reason from server)
    await cli.connect()
    cli.chat(text); cli.vote(target); cli.act(action, target)
    await cli.close()

----------------------------------------------------------------------------
SECURITY: MUTUAL TLS (optional; pass tls=TLSConfig(...) on both sides)
----------------------------------------------------------------------------
Without TLS (Project 1 behaviour) traffic is plaintext and identity is just
whatever name a client types. With TLS:

    * Encryption: every byte of every stream (including the wolves' night
      chat) is TLS-encrypted, so tcpdump on the wire shows only ciphertext.
    * Server authentication: the client verifies the server's certificate
      against the game CA, so a fake moderator can't impersonate the server.
    * Client authentication (the "mutual" part): the server REQUIRES a client
      certificate signed by the game CA. Connections without one fail the
      TLS handshake before Play() is ever called, so unauthenticated clients
      can't even open an idle stream.
    * Identity from the certificate: the player's name is the certificate's
      Common Name (CN), read from the verified TLS session. A join that
      claims a different name is rejected with PERMISSION_DENIED. Each
      player's private key is readable only by their own Linux user, so
      game identity is tied to OS identity.

    Server:  CommsServer(..., tls=TLSConfig(ca="certs/ca.crt",
                                            cert="certs/server.crt",
                                            key="certs/server.key"))
    Client:  CommsClient(..., tls=TLSConfig(ca="certs/ca.crt",
                                            cert="certs/alice.crt",
                                            key="certs/alice.key"))

Not implemented: certificate revocation (a leaked key stays valid until the
cert expires), and anyone who holds the CA key can mint any name.
"""

import asyncio
import re
import uuid
from dataclasses import dataclass

import grpc
import werewolf_pb2 as pb
import werewolf_pb2_grpc as pb_grpc


_CLOSE = object()                       # sentinel: writer should end the stream
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")


@dataclass
class TLSConfig:
    """Paths to PEM files. `ca` verifies the peer; `cert`/`key` identify us."""
    ca: str
    cert: str
    key: str

    def read(self):
        def load(p):
            with open(p, "rb") as f:
                return f.read()
        return load(self.ca), load(self.cert), load(self.key)


def _safe(fn, *args):
    """Run a game callback; a bug in game logic must not kill the stream."""
    try:
        return fn(*args)
    except Exception as e:              # noqa: BLE001 - log and keep serving
        print(f"[comms] callback {getattr(fn, '__name__', fn)} raised: {e!r}")
        return None


# ===========================================================================
# SERVER
# ===========================================================================
class _Conn:
    """Per-player connection state."""
    __slots__ = ("player_id", "name", "q", "alive", "kill")

    def __init__(self, player_id, name, outbound_max):
        self.player_id = player_id
        self.name = name
        self.q = asyncio.Queue(maxsize=outbound_max)
        self.alive = True
        self.kill = asyncio.Event()     # hard close (queue full / shutdown)


class CommsServer(pb_grpc.WerewolfServicer):
    def __init__(self, host="0.0.0.0", port=50051, max_players=32,
                 join_timeout=10.0, max_pending=256, max_msg_bytes=4096,
                 outbound_max=256, chat_min_interval=0.25, tls=None):
        self._tls = tls
        self._host = host
        self._port = port
        self._max_players = max_players
        self._join_timeout = join_timeout
        self._max_pending = max_pending
        self._max_msg_bytes = max_msg_bytes
        self._outbound_max = outbound_max
        self._chat_min_interval = chat_min_interval

        self._conns = {}                # player_id -> _Conn
        self._pending = 0               # streams that have not joined yet
        self._server = None

        self.on_join = lambda pid, name: None
        self.on_chat = lambda pid, text: None
        self.on_vote = lambda pid, target: None
        self.on_action = lambda pid, action, target: None
        self.on_leave = lambda pid: None

    # ---- lifecycle -------------------------------------------------------
    async def start(self):
        self._server = grpc.aio.server(
            # hard ceiling on concurrent RPCs; extras get RESOURCE_EXHAUSTED
            maximum_concurrent_rpcs=self._max_players + self._max_pending,
            options=[
                # oversized frames are rejected by gRPC before we see them
                ("grpc.max_receive_message_length", self._max_msg_bytes),
                # HTTP/2 keepalive pings detect half-open TCP connections
                # (client machine vanished) so on_leave still fires
                ("grpc.keepalive_time_ms", 20_000),
                ("grpc.keepalive_timeout_ms", 10_000),
                ("grpc.http2.max_pings_without_data", 0),
            ],
        )
        pb_grpc.add_WerewolfServicer_to_server(self, self._server)
        addr = f"{self._host}:{self._port}"
        if self._tls is None:
            self._server.add_insecure_port(addr)
        else:
            ca, cert, key = self._tls.read()
            creds = grpc.ssl_server_credentials(
                [(key, cert)],
                root_certificates=ca,          # trust only our game CA
                require_client_auth=True,      # mutual TLS: no cert, no entry
            )
            self._server.add_secure_port(addr, creds)
        await self._server.start()

    @property
    def secure(self):
        return self._tls is not None

    @staticmethod
    def _peer_cn(context):
        """The verified certificate Common Name of this stream's client."""
        cn = context.auth_context().get("x509_common_name")
        return cn[0].decode() if cn else None

    async def stop(self, grace=1.0):
        for pid in list(self._conns):
            self.disconnect(pid)
        if self._server is not None:
            await self._server.stop(grace)

    # ---- the one RPC (bidi stream), as a coroutine ----------------------
    async def Play(self, request_iterator, context):
        # ---------- phase 1: unauthenticated, pre-join ----------
        if self._pending >= self._max_pending:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED,
                                "server busy, try again")
        self._pending += 1
        try:
            # FIX (JP #2): bounded wait. P1 did `next(request_iterator)` with
            # no timeout while holding a thread.
            first = await asyncio.wait_for(context.read(), self._join_timeout)
        except asyncio.TimeoutError:
            first = None
        finally:
            self._pending -= 1

        if first is None:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,
                                f"no join within {self._join_timeout:g}s")
        if first is grpc.aio.EOF or first.WhichOneof("kind") != "join":
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                "first message must be a join")

        # FIX (JP #1): validated, unique names. Votes resolve by name, so two
        # players called "alice" made votes ambiguous/spoofable in P1.
        name = first.join.name.strip()
        if self._tls is not None:
            # identity comes from the certificate, not from what was typed
            cn = self._peer_cn(context)
            if cn is None:
                await context.abort(grpc.StatusCode.UNAUTHENTICATED,
                                    "no client certificate")
            if name and name.lower() != cn.lower():
                print(f"[comms] spoof attempt: cert CN={cn} claimed '{name}'")
                await context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    f"your certificate says you are '{cn}', not '{name}'")
            name = cn
        if not NAME_RE.match(name):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                                "name must be 1-16 chars of A-Z a-z 0-9 _ -")
        if len(self._conns) >= self._max_players:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "game full")
        if any(c.name.lower() == name.lower() for c in self._conns.values()):
            await context.abort(grpc.StatusCode.ALREADY_EXISTS,
                                f"name '{name}' is taken")

        pid = uuid.uuid4().hex[:8]
        conn = _Conn(pid, name, self._outbound_max)
        self._conns[pid] = conn
        reason = _safe(self.on_join, pid, name)
        if reason:
            self._conns.pop(pid, None)
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, reason)

        # ---------- phase 2: joined player ----------
        loop = asyncio.get_running_loop()

        async def reader():
            last_chat = 0.0
            while True:
                msg = await context.read()
                if msg is grpc.aio.EOF:
                    return                      # client closed its side
                kind = msg.WhichOneof("kind")
                if kind == "chat":
                    now = loop.time()
                    if now - last_chat < self._chat_min_interval:
                        continue                # rate limit: drop the line
                    last_chat = now
                    _safe(self.on_chat, pid, msg.chat.text)
                elif kind == "vote":
                    _safe(self.on_vote, pid, msg.vote.target)
                elif kind == "action":
                    _safe(self.on_action, pid, msg.action.action,
                          msg.action.target)
                # a second join on the same stream is ignored

        async def writer():
            while True:
                item = await conn.q.get()
                if item is _CLOSE:
                    return
                await context.write(item)

        tasks = [asyncio.create_task(reader()),
                 asyncio.create_task(writer()),
                 asyncio.create_task(conn.kill.wait())]
        try:
            # whichever finishes first ends the session:
            #   reader  -> client hung up      writer -> server disconnected it
            #   kill    -> hard close (slow client / shutdown)
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            # also reached via CancelledError when gRPC sees the peer vanish
            for t in tasks:
                t.cancel()
            conn.alive = False
            if self._conns.pop(pid, None) is not None:
                _safe(self.on_leave, pid)       # FIX (JP #3): game is told

    # ---- outbound comms patterns ----------------------------------------
    @staticmethod
    def _msg(mtype, text, scope, phase, sender):
        return pb.ServerMsg(type=mtype, scope=scope, phase=phase,
                            sender=sender, text=text)

    def _enqueue(self, conn, m):
        if not conn.alive:
            return
        try:
            conn.q.put_nowait(m)
        except asyncio.QueueFull:
            # slow consumer: drop it rather than buffer without bound
            print(f"[comms] {conn.name} too slow; disconnecting")
            conn.alive = False
            conn.kill.set()

    def send(self, player_id, text, mtype=pb.ServerMsg.INFO,
             scope=pb.ServerMsg.PRIVATE, phase="", sender="MODERATOR"):
        """UNICAST: deliver to exactly one player's stream."""
        conn = self._conns.get(player_id)
        if conn:
            self._enqueue(conn, self._msg(mtype, text, scope, phase, sender))

    def multicast(self, player_ids, text, mtype=pb.ServerMsg.INFO,
                  scope=pb.ServerMsg.WOLVES, phase="", sender="MODERATOR"):
        """MULTICAST: deliver to a subset of players (wolves, or a GROUP).
        gRPC has no native multicast; this fans out across held streams."""
        m = self._msg(mtype, text, scope, phase, sender)
        for pid in player_ids:
            conn = self._conns.get(pid)
            if conn:
                self._enqueue(conn, m)

    def broadcast(self, text, mtype=pb.ServerMsg.INFO,
                  scope=pb.ServerMsg.ALL, phase="", sender="MODERATOR"):
        """BROADCAST: deliver to every connected player's stream."""
        m = self._msg(mtype, text, scope, phase, sender)
        for conn in list(self._conns.values()):
            self._enqueue(conn, m)

    # ---- helpers ---------------------------------------------------------
    def names(self):
        return {pid: c.name for pid, c in self._conns.items()}

    def disconnect(self, player_id):
        """Close one stream after flushing what is already queued for it."""
        conn = self._conns.get(player_id)
        if not conn:
            return
        conn.alive = False
        try:
            conn.q.put_nowait(_CLOSE)
        except asyncio.QueueFull:
            conn.kill.set()


# ===========================================================================
# CLIENT
# ===========================================================================
class CommsClient:
    def __init__(self, host="127.0.0.1", port=50051, name="", tls=None):
        # with TLS, name may be "" — the server takes it from our certificate
        self._addr = f"{host}:{port}"
        self._name = name
        self._tls = tls
        self._channel = None
        self._call = None
        self._out = None
        self._tasks = []
        self.on_event = lambda msg: None
        self.on_close = lambda reason: None

    async def connect(self):
        self._out = asyncio.Queue()
        if self._tls is None:
            self._channel = grpc.aio.insecure_channel(self._addr)
        else:
            ca, cert, key = self._tls.read()
            creds = grpc.ssl_channel_credentials(
                root_certificates=ca,          # verify the server against our CA
                private_key=key,               # prove who we are
                certificate_chain=cert)
            self._channel = grpc.aio.secure_channel(self._addr, creds)
        stub = pb_grpc.WerewolfStub(self._channel)
        self._call = stub.Play()                  # open the bidi stream
        # the mandatory join goes first, before anything else is queued.
        # Under TLS a failed handshake surfaces here or on the first read.
        try:
            await self._call.write(
                pb.ClientMsg(join=pb.JoinMsg(name=self._name)))
        except Exception:
            pass                                  # reader reports the reason
        self._tasks = [asyncio.create_task(self._read_loop()),
                       asyncio.create_task(self._write_loop())]

    async def _read_loop(self):
        reason = "server closed the stream"
        try:
            while True:
                ev = await self._call.read()
                if ev is grpc.aio.EOF:
                    break
                _safe(self.on_event, ev)      # a UI bug must not kill the stream
        except asyncio.CancelledError:
            reason = None                         # we closed it ourselves
            raise
        except grpc.aio.AioRpcError as e:
            reason = f"{e.code().name}: {e.details()}"
        finally:
            if reason:
                self.on_close(reason)

    async def _write_loop(self):
        # single writer: gRPC allows only one outstanding write per call
        try:
            while True:
                item = await self._out.get()
                if item is _CLOSE:
                    await self._call.done_writing()
                    return
                await self._call.write(item)
        except asyncio.CancelledError:
            raise
        except Exception:                         # stream already finished
            return

    def chat(self, text):
        self._out.put_nowait(pb.ClientMsg(chat=pb.ChatMsg(text=text)))

    def vote(self, target):
        self._out.put_nowait(pb.ClientMsg(vote=pb.VoteMsg(target=target)))

    def act(self, action, target):
        self._out.put_nowait(pb.ClientMsg(
            action=pb.ActionMsg(action=action, target=target)))

    async def close(self):
        if self._out is not None:
            self._out.put_nowait(_CLOSE)
            await asyncio.sleep(0.1)              # let the half-close flush
        for t in self._tasks:
            t.cancel()
        if self._channel is not None:
            await self._channel.close()
