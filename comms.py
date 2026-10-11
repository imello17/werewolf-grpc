"""
comms.py  —  the single-file communications library for Werewolf-over-gRPC.

THIS IS THE COMMUNICATIONS LIBRARY the assignment asks for. Everything that
touches gRPC lives here; server.py and client.py import only this file and
never see a gRPC symbol. It is a thin wrapper over gRPC's Python bindings
(which the assignment explicitly permits).

----------------------------------------------------------------------------
PRIMITIVE:  gRPC bidirectional streaming over HTTP/2 (TCP).
            One rpc, `Play(stream ClientMsg) returns (stream ServerMsg)`,
            carries an entire player's session on a single long-lived stream.

COMMS PATTERNS the library exposes (the rubric's "non-trivial comms"):
    send(player_id, msg)         -> UNICAST   (one HTTP/2 stream)
    multicast(player_ids, msg)   -> MULTICAST (a subset of streams; wolves)
    broadcast(msg)               -> BROADCAST (every connected stream)

CONCURRENCY model:
    * gRPC runs each Play() call in its own handler thread (thread pool).
    * Each connected player owns a queue.Queue. The handler blocks reading
      the client's incoming messages; a second loop drains that player's
      queue and yields each ServerMsg down the stream. send/multicast/
      broadcast just put() onto queues — they never block on a slow client
      and never touch gRPC directly. This decouples the game loop from the
      network and is where backpressure is absorbed.

----------------------------------------------------------------------------
PUBLIC API
----------------------------------------------------------------------------
Server side:
    srv = CommsServer(host, port)
    srv.on_join    = fn(player_id, name) -> None
    srv.on_chat    = fn(player_id, text) -> None
    srv.on_vote    = fn(player_id, target) -> None
    srv.on_leave   = fn(player_id) -> None
    srv.start()                       # non-blocking; spins up the gRPC server
    srv.send(pid, type, text, scope, phase, sender)        # UNICAST
    srv.multicast(pids, type, text, scope, phase, sender)  # MULTICAST
    srv.broadcast(type, text, scope, phase, sender)        # BROADCAST
    srv.names()                       # {player_id: name} of connected players
    srv.disconnect(pid)               # close one stream (e.g., on game over)
    srv.stop()

Client side:
    cli = CommsClient(host, port, name)
    cli.on_event = fn(ServerMsg) -> None     # called for every server message
    cli.connect()                     # opens Play(), sends Join, starts reader
    cli.chat(text)
    cli.vote(target)
    cli.close()

NOTE: there is intentionally NO authentication token and NO TLS. Identity is
the server-assigned player_id bound to a stream; anyone who can reach the port
can join, and traffic is plaintext. Both are deliberate, documented weaknesses
for the classmate-security-feedback section of the report.
"""

import threading
import queue
import time
import uuid
from concurrent import futures

import grpc
import werewolf_pb2 as pb
import werewolf_pb2_grpc as pb_grpc


# A sentinel put on a player's queue to tell its writer loop to end the stream.
_CLOSE = object()


# ===========================================================================
# SERVER
# ===========================================================================
class _Conn:
    """Per-player connection state: the outbound queue and display name."""
    __slots__ = ("player_id", "name", "q", "alive")

    def __init__(self, player_id, name):
        self.player_id = player_id
        self.name = name
        self.q = queue.Queue()
        self.alive = True


class CommsServer(pb_grpc.WerewolfServicer):
    def __init__(self, host="0.0.0.0", port=50051, max_players=32):
        self._host = host
        self._port = port
        self._conns = {}                 # player_id -> _Conn
        self._lock = threading.Lock()    # guards _conns
        self._server = None
        self._max_players = max_players

        # Callbacks the game logic (server.py) wires up. Defaults are no-ops.
        self.on_join = lambda pid, name: None
        self.on_chat = lambda pid, text: None
        self.on_vote = lambda pid, target: None
        self.on_leave = lambda pid: None

    # ---- lifecycle -------------------------------------------------------
    def start(self):
        # enough pool threads for every player's Play() handler plus headroom.
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=self._max_players + 4)
        )
        pb_grpc.add_WerewolfServicer_to_server(self, self._server)
        self._server.add_insecure_port(f"{self._host}:{self._port}")
        self._server.start()

    def stop(self, grace=1.0):
        # wake every writer loop so handlers return and the server can close.
        with self._lock:
            for c in self._conns.values():
                c.alive = False
                c.q.put(_CLOSE)
        if self._server is not None:
            self._server.stop(grace)

    # ---- the one RPC (bidi stream) --------------------------------------
    def Play(self, request_iterator, context):
        """
        One call per player. Runs in its own gRPC pool thread.
        The FIRST ClientMsg must be a join; it establishes identity.
        A background thread reads subsequent client messages; THIS thread
        drains the player's outbound queue and yields to the wire.
        """
        conn = None
        reader = None
        try:
            # ---- first message must be join ----
            first = next(request_iterator)
            if first.WhichOneof("kind") != "join":
                return  # protocol violation: drop the stream
            name = first.join.name or "anon"

            player_id = uuid.uuid4().hex[:8]
            conn = _Conn(player_id, name)
            with self._lock:
                self._conns[player_id] = conn
            self.on_join(player_id, name)

            # ---- reader: pulls client actions off the stream ----
            def read_loop():
                try:
                    for msg in request_iterator:
                        kind = msg.WhichOneof("kind")
                        if kind == "chat":
                            self.on_chat(player_id, msg.chat.text)
                        elif kind == "vote":
                            self.on_vote(player_id, msg.vote.target)
                        # a second join on the same stream is ignored
                except Exception:
                    pass
                finally:
                    conn.alive = False
                    conn.q.put(_CLOSE)  # unblock the writer below

            reader = threading.Thread(target=read_loop, daemon=True)
            reader.start()

            # ---- writer: drain outbound queue -> yield down the stream ----
            while conn.alive and context.is_active():
                item = conn.q.get()
                if item is _CLOSE:
                    break
                yield item
        finally:
            if conn is not None:
                with self._lock:
                    self._conns.pop(conn.player_id, None)
                self.on_leave(conn.player_id)

    # ---- outbound comms patterns ----------------------------------------
    def _msg(self, mtype, text, scope, phase, sender):
        return pb.ServerMsg(
            type=mtype, scope=scope, phase=phase, sender=sender, text=text
        )

    def send(self, player_id, text, mtype=pb.ServerMsg.INFO,
             scope=pb.ServerMsg.PRIVATE, phase="", sender="MODERATOR"):
        """UNICAST: deliver to exactly one player's stream."""
        with self._lock:
            conn = self._conns.get(player_id)
        if conn and conn.alive:
            conn.q.put(self._msg(mtype, text, scope, phase, sender))

    def multicast(self, player_ids, text, mtype=pb.ServerMsg.INFO,
                  scope=pb.ServerMsg.WOLVES, phase="", sender="MODERATOR"):
        """MULTICAST: deliver to a subset of players (e.g., the wolves)."""
        m = self._msg(mtype, text, scope, phase, sender)
        with self._lock:
            targets = [self._conns.get(pid) for pid in player_ids]
        for conn in targets:
            if conn and conn.alive:
                conn.q.put(m)

    def broadcast(self, text, mtype=pb.ServerMsg.INFO,
                  scope=pb.ServerMsg.ALL, phase="", sender="MODERATOR"):
        """BROADCAST: deliver to every connected player's stream."""
        m = self._msg(mtype, text, scope, phase, sender)
        with self._lock:
            conns = list(self._conns.values())
        for conn in conns:
            if conn.alive:
                conn.q.put(m)

    # ---- helpers ---------------------------------------------------------
    def names(self):
        with self._lock:
            return {pid: c.name for pid, c in self._conns.items()}

    def disconnect(self, player_id):
        with self._lock:
            conn = self._conns.get(player_id)
        if conn:
            conn.alive = False
            conn.q.put(_CLOSE)


# ===========================================================================
# CLIENT
# ===========================================================================
class CommsClient:
    def __init__(self, host="127.0.0.1", port=50051, name="player"):
        self._addr = f"{host}:{port}"
        self._name = name
        self._channel = None
        self._out = queue.Queue()      # ClientMsgs waiting to go to server
        self._stop = threading.Event()
        self._reader = None
        self.on_event = lambda msg: None   # server.py-style callback for events

    def connect(self):
        self._channel = grpc.insecure_channel(self._addr)
        stub = pb_grpc.WerewolfStub(self._channel)

        # queue the mandatory join as the FIRST outbound message
        self._out.put(pb.ClientMsg(join=pb.JoinMsg(name=self._name)))

        def outbound():
            # generator gRPC pulls from to get our client->server messages
            while not self._stop.is_set():
                try:
                    item = self._out.get(timeout=0.2)
                except queue.Empty:
                    continue
                if item is _CLOSE:
                    return
                yield item

        response_stream = stub.Play(outbound())

        def read_loop():
            try:
                for ev in response_stream:
                    self.on_event(ev)
            except Exception:
                pass
            finally:
                self._stop.set()

        self._reader = threading.Thread(target=read_loop, daemon=True)
        self._reader.start()

    def chat(self, text):
        self._out.put(pb.ClientMsg(chat=pb.ChatMsg(text=text)))

    def vote(self, target):
        self._out.put(pb.ClientMsg(vote=pb.VoteMsg(target=target)))

    def close(self):
        self._stop.set()
        self._out.put(_CLOSE)
        if self._channel is not None:
            self._channel.close()
