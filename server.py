#!/usr/bin/env python3
"""
server.py — the moderator. The game loop is a coroutine on the same asyncio
event loop that serves every player's stream (no extra threads, no locks).

It NEVER touches gRPC: all I/O is through comms.CommsServer's
send / multicast / broadcast (unicast / multicast / broadcast).

Roles:  WOLF, TOWN, DOCTOR (at >= --doctor-min players),
        JESTER (at >= --jester-min players).

Round:
  NIGHT  — three things happen CONCURRENTLY (asyncio.gather):
             wolves:  private chat window, then a vote on a victim
             doctor:  /save NAME  (may self-save; not the same person twice
                      in a row)
             jester's ghost (only the night after being lynched):
                      /revenge NAME among the players who voted for him
           Resolution: a wolf kill on the doctor's target is prevented; the
           jester's revenge cannot be blocked.
  DAY    — open chat, then a town vote. If the town lynches the JESTER, the
           jester wins (individually, the game continues) and haunts the
           next night.
  WIN    — TOWN when no wolves remain; WOLVES when wolves >= town-side
           (town + doctor). The jester is neutral and counts for neither.

Vote windows close early once every eligible voter has voted.

Usage:  python3 server.py [--host 0.0.0.0] [--port 50051] [--players N]
                          [--tls certs/] [--fast] [--seed S] [--doctor-min 5] [--jester-min 7]
"""
import argparse
import asyncio
import random
import time

import comms
import werewolf_pb2 as pb

T = pb.ServerMsg          # shorthand for message types / scopes
ROLE_LABEL = {"wolf": "WOLF", "town": "TOWNSPERSON",
              "doctor": "DOCTOR", "jester": "JESTER"}

TIMINGS = {               # seconds; --fast divides them by 5
    "lobby": 20, "wolf_chat": 15, "night_vote": 15,
    "day_chat": 20, "town_vote": 20,
}


class Game:
    def __init__(self, srv, min_players, timings, doctor_min, jester_min, rng):
        self.srv = srv
        self.min_players = min_players
        self.t = timings
        self.doctor_min = doctor_min
        self.jester_min = jester_min
        self.rng = rng

        self.players = {}        # pid -> name (everyone who joined)
        self.roles = {}          # pid -> "wolf" | "town" | "doctor" | "jester"
        self.alive = set()
        self.phase = "lobby"
        self.started = False

        # voting
        self.votes = {}          # voter pid -> target pid
        self.eligible = set()    # who may vote in the current window
        self.all_voted = asyncio.Event()

        # doctor
        self.doctor_choice = None
        self.doctor_last = None  # who was saved last night
        self.doctor_evt = asyncio.Event()

        # jester
        self.jester_won = False
        self.revenge_pending = False
        self.jester_voters = []  # pids who voted to lynch the jester
        self.revenge_choice = None
        self.revenge_evt = asyncio.Event()

        srv.on_join = self.on_join
        srv.on_chat = self.on_chat
        srv.on_vote = self.on_vote
        srv.on_action = self.on_action
        srv.on_leave = self.on_leave

    # ---------- helpers ----------
    def name(self, pid):
        return self.players.get(pid, "?")

    def pid_of(self, name):
        name = (name or "").strip().lower()
        for pid, n in self.players.items():
            if n.lower() == name:
                return pid
        return None

    def with_role(self, *roles):
        return [p for p in self.alive if self.roles.get(p) in roles]

    def wolves(self):
        return self.with_role("wolf")

    def town_side(self):
        return self.with_role("town", "doctor")

    def alive_names(self):
        return ", ".join(sorted(self.name(p) for p in self.alive))

    def winner(self):
        w, t = len(self.wolves()), len(self.town_side())
        if w == 0:
            return "TOWN"
        if w >= t:
            return "WOLVES"
        return None

    # ---------- comms callbacks (all run on the event loop) ----------
    def on_join(self, pid, name):
        if self.started:
            return "game already in progress, try later"
        self.players[pid] = name
        self.alive.add(pid)
        print(f"[join] {name} ({pid})")
        self.srv.send(pid, f"Welcome {name}. Waiting for the game to start.",
                      phase="lobby")
        self.srv.broadcast(f"{name} joined ({len(self.players)} players).",
                           phase="lobby")
        return None

    def on_leave(self, pid):
        """FIX (JP #3): a disconnect now removes the player from the game."""
        name = self.name(pid)
        print(f"[leave] {name} ({pid})")
        if not self.started:
            self.players.pop(pid, None)
            self.alive.discard(pid)
            self.srv.broadcast(f"{name} left the lobby.", phase="lobby")
            return
        if pid not in self.alive or self.phase == "gameover":
            return
        self.alive.discard(pid)
        self.eligible.discard(pid)
        self.votes.pop(pid, None)
        # votes AT the departed player no longer count either
        self.votes = {v: t for v, t in self.votes.items() if t != pid}
        self._check_all_voted()
        self.srv.broadcast(
            f"{name} disconnected and is out. They were a "
            f"{ROLE_LABEL.get(self.roles.get(pid), '?')}.",
            mtype=T.DEATH, phase=self.phase)

    def on_chat(self, pid, text):
        if pid not in self.alive:
            return
        text = text[:300]
        if self.phase == "wolf_chat" and self.roles.get(pid) == "wolf":
            self.srv.multicast(self.wolves(), text, mtype=T.CHAT,          # MULTICAST
                               scope=T.WOLVES, phase=self.phase,
                               sender=self.name(pid))
        elif self.phase == "day_chat":
            self.srv.broadcast(text, mtype=T.CHAT, scope=T.ALL,           # BROADCAST
                               phase=self.phase, sender=self.name(pid))

    def on_vote(self, pid, target_name):
        if self.phase not in ("wolf_vote", "town_vote"):
            self.srv.send(pid, "Not a voting phase.")
            return
        if pid not in self.eligible:
            return                       # e.g. a townsperson during wolf_vote
        tgt = self.pid_of(target_name)
        if tgt is None or tgt not in self.alive:
            self.srv.send(pid, f"Invalid target: {target_name}")
            return
        self.votes[pid] = tgt
        self.srv.send(pid, f"Vote for {self.name(tgt)} recorded.")
        print(f"[vote] {self.name(pid)} -> {self.name(tgt)} ({self.phase})")
        self._check_all_voted()

    def on_action(self, pid, action, target_name):
        tgt = self.pid_of(target_name)
        if action == "save":
            if self.roles.get(pid) != "doctor" or pid not in self.alive:
                return self.srv.send(pid, "Only the living doctor can save.")
            if not self.phase.startswith("wolf_"):
                return self.srv.send(pid, "You can only save at night.")
            if self.doctor_evt.is_set():
                return self.srv.send(pid, "You already chose tonight.")
            if tgt is None or tgt not in self.alive:
                return self.srv.send(pid, f"Invalid target: {target_name}")
            if tgt == self.doctor_last:
                return self.srv.send(
                    pid, "You can't save the same person two nights in a row.")
            self.doctor_choice = tgt
            self.doctor_evt.set()
            self.srv.send(pid, f"You will watch over {self.name(tgt)} tonight.")
            print(f"[doctor] saves {self.name(tgt)}")
        elif action == "revenge":
            if not (self.revenge_pending and self.roles.get(pid) == "jester"):
                return self.srv.send(pid, "You have no revenge to take.")
            if not self.phase.startswith("wolf_"):
                return self.srv.send(pid, "Revenge happens at night.")
            if self.revenge_evt.is_set():
                return self.srv.send(pid, "You already chose.")
            if tgt not in self.jester_voters or tgt not in self.alive:
                return self.srv.send(
                    pid, "You can only take someone who voted for you: "
                    + ", ".join(self.name(p) for p in self.jester_voters
                                if p in self.alive))
            self.revenge_choice = tgt
            self.revenge_evt.set()
            self.srv.send(pid, f"{self.name(tgt)} will not see the morning.")
            print(f"[jester] revenge on {self.name(tgt)}")
        else:
            self.srv.send(pid, f"Unknown action: {action}")

    # ---------- voting machinery ----------
    def _open_vote(self, phase, voters):
        self.phase = phase
        self.votes = {}
        self.eligible = set(voters)
        self.all_voted = asyncio.Event()
        self._check_all_voted()

    def _check_all_voted(self):
        if self.eligible and self.eligible <= set(self.votes):
            self.all_voted.set()

    async def _vote_window(self, seconds):
        """Wait until everyone eligible has voted or time runs out."""
        try:
            await asyncio.wait_for(self.all_voted.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    def _tally(self):
        counts = {}
        for tgt in self.votes.values():
            counts[tgt] = counts.get(tgt, 0) + 1
        if not counts:
            return None
        top = max(counts.values())
        leaders = [p for p, c in counts.items() if c == top]
        return leaders[0] if len(leaders) == 1 else None

    def kill(self, pid, how):
        self.alive.discard(pid)
        role = ROLE_LABEL.get(self.roles.get(pid), "?")
        self.srv.broadcast(f"{self.name(pid)} {how}. They were a {role}.",
                           mtype=T.DEATH, phase=self.phase)
        self.srv.send(pid, "YOU ARE OUT. You may keep watching.",
                      mtype=T.DEATH)

    # ---------- the three concurrent night actors ----------
    async def _wolves_night(self):
        wolves = self.wolves()
        self.phase = "wolf_chat"
        self.srv.multicast(wolves,
                           f"Wolves, discuss ({self.t['wolf_chat']}s). "
                           f"Targets: {self.alive_names()}",
                           mtype=T.PROMPT, scope=T.WOLVES, phase=self.phase)
        await asyncio.sleep(self.t["wolf_chat"])
        self._open_vote("wolf_vote", self.wolves())
        self.srv.multicast(self.wolves(),
                           f"Wolves, /vote NAME ({self.t['night_vote']}s).",
                           mtype=T.PROMPT, scope=T.WOLVES, phase=self.phase)
        await self._vote_window(self.t["night_vote"])
        return self._tally()

    async def _doctor_night(self, night_len):
        self.doctor_choice = None
        self.doctor_evt = asyncio.Event()
        doctors = self.with_role("doctor")
        if not doctors:
            return None
        banned = (f" (not {self.name(self.doctor_last)} again)"
                  if self.doctor_last in self.alive else "")
        self.srv.send(doctors[0],
                      f"Doctor, /save NAME within {night_len}s{banned}. "
                      f"Alive: {self.alive_names()}",
                      mtype=T.PROMPT, phase="night")
        try:
            await asyncio.wait_for(self.doctor_evt.wait(), night_len)
        except asyncio.TimeoutError:
            self.srv.send(doctors[0], "You slept through the night.")
        return self.doctor_choice

    async def _jester_night(self, night_len):
        self.revenge_choice = None
        self.revenge_evt = asyncio.Event()
        if not self.revenge_pending:
            return None
        jester = next(p for p, r in self.roles.items() if r == "jester")
        candidates = [p for p in self.jester_voters if p in self.alive]
        if not candidates:
            return None
        self.srv.send(jester,
                      f"Your ghost rises. /revenge NAME within {night_len}s. "
                      f"Choose from: {', '.join(self.name(p) for p in candidates)}",
                      mtype=T.PROMPT, phase="night")
        try:
            await asyncio.wait_for(self.revenge_evt.wait(), night_len)
        except asyncio.TimeoutError:
            self.revenge_choice = self.rng.choice(candidates)
            self.srv.send(jester, "You hesitated; fate chose "
                          f"{self.name(self.revenge_choice)}.")
        return self.revenge_choice

    # ---------- the game loop ----------
    async def run(self):
        self.srv.broadcast(f"Lobby open. Game starts in {self.t['lobby']}s.",
                           phase="lobby")
        t0 = time.monotonic()
        while (time.monotonic() - t0 < self.t["lobby"]
               or len(self.players) < self.min_players):
            await asyncio.sleep(0.5)
            if time.monotonic() - t0 > self.t["lobby"] * 3:
                break

        self.started = True
        # sort by name so --seed gives the same roles every run
        pids = sorted(self.alive, key=lambda p: self.name(p).lower())
        if len(pids) < 2:
            self.srv.broadcast("Not enough players. Shutting down.",
                               mtype=T.GAMEOVER)
            return

        self._assign_roles(pids)

        round_no = 0
        while self.winner() is None:
            round_no += 1
            self.srv.broadcast(f"===== ROUND {round_no} =====", phase="night")
            await self._night()
            if self.winner() is not None:
                break
            await self._day()

        who = self.winner()
        self.phase = "gameover"
        extra = " The JESTER also won." if self.jester_won else ""
        self.srv.broadcast(f"GAME OVER — {who} WIN.{extra}",
                           mtype=T.GAMEOVER, phase="gameover")
        print(f"[end] {who} win{extra}")
        await asyncio.sleep(1)
        for pid in list(self.players):
            self.srv.disconnect(pid)

    def _assign_roles(self, pids):
        self.rng.shuffle(pids)
        n = len(pids)
        n_wolves = max(1, n // 3)
        deck = ["wolf"] * n_wolves
        if n >= self.doctor_min:
            deck.append("doctor")
        if n >= self.jester_min:
            deck.append("jester")
        deck += ["town"] * (n - len(deck))
        blurbs = {
            "wolf": "You are a WOLF. Each night, agree with your pack on a victim.",
            "town": "You are a TOWNSPERSON. Find the wolves and vote them out.",
            "doctor": "You are the DOCTOR. Each night, /save NAME to protect "
                      "someone from the wolves. You may save yourself, but not "
                      "the same person two nights in a row.",
            "jester": "You are the JESTER. You win if the town lynches you. If "
                      "they do, your ghost kills one of the players who voted "
                      "for you the following night.",
        }
        for pid, role in zip(pids, deck):
            self.roles[pid] = role
            self.srv.send(pid, blurbs[role], mtype=T.ROLE, phase="setup")   # UNICAST
        wolves = self.wolves()
        self.srv.multicast(wolves, "Your pack: "                            # MULTICAST
                           + ", ".join(self.name(p) for p in wolves),
                           scope=T.WOLVES, phase="setup")
        print("[roles]", {self.name(p): self.roles[p] for p in pids})
        specials = [r.upper() for r in ("doctor", "jester") if r in deck]
        self.srv.broadcast(
            f"{n} players: {n_wolves} wolves"
            + (f", plus a {' and a '.join(specials)}" if specials else "")
            + ". Good luck.", phase="setup")

    async def _night(self):
        self.srv.broadcast("Night falls. Town, close your eyes.",
                           mtype=T.PHASE, phase="night")
        night_len = self.t["wolf_chat"] + self.t["night_vote"]
        # the heart of the async design: three independent actors, one thread
        victim, saved, revenge = await asyncio.gather(
            self._wolves_night(),
            self._doctor_night(night_len),
            self._jester_night(night_len),
        )
        self.phase = "night"
        self.eligible = set()

        if victim is not None and victim in self.alive:
            if victim == saved:
                self.srv.broadcast("The wolves attacked, but their victim "
                                   "survived the night.", phase="night")
                doc = self.with_role("doctor")
                if doc:
                    self.srv.send(doc[0], "You saved a life tonight.")
                self.srv.send(victim, "You were attacked in the night, but "
                              "the doctor saved you.")
            else:
                self.kill(victim, "was killed by the wolves")
        elif victim is None:
            self.srv.broadcast("The wolves could not agree. No one was "
                               "attacked.", phase="night")

        if saved is not None:
            self.doctor_last = saved
        if self.revenge_pending:
            self.revenge_pending = False
            if revenge is not None and revenge in self.alive:
                self.kill(revenge, "was dragged away by the jester's ghost")

    async def _day(self):
        self.srv.broadcast("Day breaks. Everyone, open your eyes.",
                           mtype=T.PHASE, phase="day")
        self.phase = "day_chat"
        self.srv.broadcast(f"Discuss ({self.t['day_chat']}s). "
                           f"Alive: {self.alive_names()}",
                           mtype=T.PROMPT, phase=self.phase)
        await asyncio.sleep(self.t["day_chat"])

        self._open_vote("town_vote", self.alive)
        self.srv.broadcast(f"/vote NAME to lynch someone "
                           f"({self.t['town_vote']}s). Valid: "
                           f"{self.alive_names()}",
                           mtype=T.PROMPT, phase=self.phase)
        await self._vote_window(self.t["town_vote"])
        hanged = self._tally()
        votes = dict(self.votes)
        self.eligible = set()
        self.phase = "day"

        if hanged is None or hanged not in self.alive:
            self.srv.broadcast("No majority. No one is lynched today.",
                               phase="day")
            return

        if self.roles.get(hanged) == "jester":
            self.jester_won = True
            self.revenge_pending = True
            self.jester_voters = [v for v, t in votes.items()
                                  if t == hanged and v != hanged]
            self.kill(hanged, "was lynched")
            self.srv.broadcast(f"{self.name(hanged)} laughs from the gallows: "
                               "the JESTER wanted this, and wins!", phase="day")
            self.srv.multicast(self.jester_voters,                         # GROUP multicast
                               "You voted for the jester. Tonight, the "
                               "jester's ghost will take one of you.",
                               scope=T.GROUP, phase="day")
        else:
            self.kill(hanged, "was lynched")


async def amain(args):
    rng = random.Random(args.seed)
    timings = dict(TIMINGS)
    if args.fast:
        timings = {k: max(1, v // 5) for k, v in timings.items()}
    tls = None
    if args.tls:
        d = args.tls.rstrip("/")
        tls = comms.TLSConfig(ca=f"{d}/ca.crt", cert=f"{d}/server.crt",
                              key=f"{d}/server.key")
    srv = comms.CommsServer(host=args.host, port=args.port,
                            max_players=args.max_players,
                            join_timeout=args.join_timeout, tls=tls)
    game = Game(srv, args.players, timings, args.doctor_min,
                args.jester_min, rng)
    await srv.start()
    mode = "mutual TLS" if tls else "INSECURE plaintext"
    print(f"[moderator] listening on {args.host}:{args.port} (asyncio, {mode})")
    try:
        await game.run()
    finally:
        await srv.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--players", type=int, default=3,
                    help="minimum players before the game starts")
    ap.add_argument("--max-players", type=int, default=16)
    ap.add_argument("--join-timeout", type=float, default=10.0,
                    help="seconds a new stream has to send its join")
    ap.add_argument("--doctor-min", type=int, default=5,
                    help="minimum players for a doctor (0 = always)")
    ap.add_argument("--jester-min", type=int, default=7,
                    help="minimum players for a jester (0 = always)")
    ap.add_argument("--seed", type=int, default=None,
                    help="RNG seed for reproducible role assignment")
    ap.add_argument("--tls", metavar="CERT_DIR", default=None,
                    help="enable mutual TLS; dir holds ca.crt, server.crt, "
                         "server.key (see make_certs.sh)")
    ap.add_argument("--fast", action="store_true",
                    help="divide all phase timers by 5 (for testing)")
    args = ap.parse_args()
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n[moderator] interrupted")


if __name__ == "__main__":
    main()
