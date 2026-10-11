#!/usr/bin/env python3
"""
server.py — the moderator. Runs the game-loop FSM on its own thread while the
comms library's thread pool handles each player's bidi stream.

It NEVER touches gRPC: all I/O is through comms.CommsServer's
send / multicast / broadcast (unicast / multicast / broadcast).

Phases: lobby -> (night: wolf chat -> wolf vote) -> day: chat -> town vote
        -> resolve -> repeat until one side wins.

Usage:  python3 server.py [--host 0.0.0.0] [--port 50051] [--players N]
"""
import argparse
import random
import threading
import time

import comms
import werewolf_pb2 as pb

# ---- tunable timings (seconds) ----
LOBBY_WAIT   = 20
WOLF_CHAT    = 15
WOLF_VOTE    = 15
DAY_CHAT     = 20
TOWN_VOTE    = 20


class Game:
    def __init__(self, srv, min_players):
        self.srv = srv
        self.min_players = min_players
        self.lock = threading.Lock()

        self.players = {}        # pid -> name
        self.roles = {}          # pid -> "wolf" | "town"
        self.alive = set()       # pids still in the game
        self.votes = {}          # pid -> target_pid (reset each vote phase)
        self.phase = "lobby"
        self.started = False

        # wire comms callbacks to game handlers
        srv.on_join = self.on_join
        srv.on_chat = self.on_chat
        srv.on_vote = self.on_vote
        srv.on_leave = self.on_leave

    # ---------- identity helpers ----------
    def pid_of(self, name):
        for pid, n in self.players.items():
            if n == name:
                return pid
        return None

    def wolves(self):
        return [p for p in self.alive if self.roles.get(p) == "wolf"]

    def town(self):
        return [p for p in self.alive if self.roles.get(p) == "town"]

    # ---------- comms callbacks ----------
    def on_join(self, pid, name):
        with self.lock:
            if self.started:
                self.srv.send(pid, "Game already in progress. Try later.",
                              mtype=pb.ServerMsg.INFO)
                self.srv.disconnect(pid)
                return
            self.players[pid] = name
            self.alive.add(pid)
        print(f"[join] {name} ({pid})")
        self.srv.send(pid, f"Welcome {name}. Waiting for the game to start.",
                      mtype=pb.ServerMsg.INFO, phase="lobby")
        self.srv.broadcast(f"{name} joined ({len(self.players)} players).",
                           mtype=pb.ServerMsg.INFO, phase="lobby")

    def on_chat(self, pid, text):
        # chat is only relayed during phases where this player is allowed to talk
        with self.lock:
            if pid not in self.alive:
                return
            name = self.players.get(pid, "?")
            phase = self.phase
            role = self.roles.get(pid)
        if phase == "wolf_chat" and role == "wolf":
            # MULTICAST: wolves-only night chat
            self.srv.multicast(self.wolves(), text, mtype=pb.ServerMsg.CHAT,
                               scope=pb.ServerMsg.WOLVES, phase=phase, sender=name)
        elif phase == "day_chat":
            # BROADCAST: open daytime discussion
            self.srv.broadcast(text, mtype=pb.ServerMsg.CHAT,
                               scope=pb.ServerMsg.ALL, phase=phase, sender=name)

    def on_vote(self, pid, target_name):
        with self.lock:
            if pid not in self.alive:
                return
            tgt = self.pid_of(target_name)
            if tgt is None or tgt not in self.alive:
                self.srv.send(pid, f"Invalid target: {target_name}")
                return
            # only wolves vote at night; everyone alive votes by day
            if self.phase == "wolf_vote" and self.roles.get(pid) != "wolf":
                return
            if self.phase not in ("wolf_vote", "town_vote"):
                self.srv.send(pid, "Not a voting phase.")
                return
            self.votes[pid] = tgt
            voter = self.players[pid]
        self.srv.send(pid, f"Vote for {target_name} recorded.")
        print(f"[vote] {voter} -> {target_name} ({self.phase})")

    def on_leave(self, pid):
        with self.lock:
            name = self.players.get(pid, "?")
        print(f"[leave] {name} ({pid})")

    # ---------- phase helpers ----------
    def set_phase(self, phase):
        with self.lock:
            self.phase = phase
            self.votes = {}

    def tally(self):
        """Return the pid with the most votes, or None on tie/no-votes."""
        with self.lock:
            counts = {}
            for tgt in self.votes.values():
                counts[tgt] = counts.get(tgt, 0) + 1
        if not counts:
            return None
        top = max(counts.values())
        leaders = [p for p, c in counts.items() if c == top]
        return leaders[0] if len(leaders) == 1 else None

    def kill(self, pid):
        with self.lock:
            self.alive.discard(pid)
            name = self.players.get(pid, "?")
            role = self.roles.get(pid, "?")
        self.srv.broadcast(f"{name} was a {role}. They are out.",
                           mtype=pb.ServerMsg.DEATH, phase=self.phase)
        self.srv.send(pid, "YOU ARE OUT. You may keep watching.",
                      mtype=pb.ServerMsg.DEATH)

    def winner(self):
        w, t = len(self.wolves()), len(self.town())
        if w == 0:
            return "TOWN"
        if w >= t:
            return "WOLVES"
        return None

    # ---------- the game loop (runs on its own thread) ----------
    def run(self):
        # wait for players
        self.srv.broadcast(f"Lobby open. Game starts in {LOBBY_WAIT}s.",
                           phase="lobby")
        t0 = time.time()
        while time.time() - t0 < LOBBY_WAIT or len(self.players) < self.min_players:
            time.sleep(0.5)
            if time.time() - t0 > LOBBY_WAIT * 3:
                break  # give up waiting for a full lobby

        with self.lock:
            self.started = True
            pids = list(self.alive)

        if len(pids) < 2:
            self.srv.broadcast("Not enough players. Shutting down.",
                               mtype=pb.ServerMsg.GAMEOVER)
            return

        # assign roles: ~1 wolf per 3 players, at least 1
        random.shuffle(pids)
        n_wolves = max(1, len(pids) // 3)
        for i, pid in enumerate(pids):
            self.roles[pid] = "wolf" if i < n_wolves else "town"
            # UNICAST: private role reveal
            self.srv.send(pid, f"You are a {self.roles[pid].upper()}.",
                          mtype=pb.ServerMsg.ROLE, phase="setup")
        print("[roles]", {self.players[p]: self.roles[p] for p in pids})
        self.srv.broadcast(
            f"{n_wolves} wolves, {len(pids)-n_wolves} townsfolk. Good luck.",
            phase="setup")

        round_no = 0
        while self.winner() is None:
            round_no += 1
            self.srv.broadcast(f"===== ROUND {round_no} =====", phase="night")

            # ---------- NIGHT ----------
            self.srv.broadcast("Night falls. Town, close your eyes.",
                               mtype=pb.ServerMsg.PHASE, phase="night")
            # wolf chat (MULTICAST window)
            self.set_phase("wolf_chat")
            self.srv.multicast(self.wolves(),
                               f"Wolves, discuss. {WOLF_CHAT}s. "
                               f"Targets: {self._alive_names()}",
                               mtype=pb.ServerMsg.PROMPT,
                               scope=pb.ServerMsg.WOLVES, phase="wolf_chat")
            time.sleep(WOLF_CHAT)
            # wolf vote
            self.set_phase("wolf_vote")
            self.srv.multicast(self.wolves(),
                               f"Wolves, vote your victim. {WOLF_VOTE}s.",
                               mtype=pb.ServerMsg.PROMPT,
                               scope=pb.ServerMsg.WOLVES, phase="wolf_vote")
            time.sleep(WOLF_VOTE)
            victim = self.tally()
            if victim is not None:
                self.srv.broadcast(f"The wolves struck in the night.",
                                   phase="night")
                self.kill(victim)
            else:
                self.srv.broadcast("The wolves could not agree. No one died.",
                                   phase="night")

            if self.winner() is not None:
                break

            # ---------- DAY ----------
            self.srv.broadcast("Day breaks. Everyone, open your eyes.",
                               mtype=pb.ServerMsg.PHASE, phase="day")
            self.set_phase("day_chat")
            self.srv.broadcast(f"Discuss. {DAY_CHAT}s. Alive: {self._alive_names()}",
                               mtype=pb.ServerMsg.PROMPT, phase="day_chat")
            time.sleep(DAY_CHAT)
            # town vote (everyone alive)
            self.set_phase("town_vote")
            self.srv.broadcast(f"Vote to hang someone. {TOWN_VOTE}s. "
                               f"Valid: {self._alive_names()}",
                               mtype=pb.ServerMsg.PROMPT, phase="town_vote")
            time.sleep(TOWN_VOTE)
            hanged = self.tally()
            if hanged is not None:
                self.srv.broadcast("The town has made its decision.", phase="day")
                self.kill(hanged)
            else:
                self.srv.broadcast("The vote was tied. No one hangs today.",
                                   phase="day")

        # ---------- END ----------
        who = self.winner()
        self.srv.broadcast(f"GAME OVER — {who} WIN.",
                           mtype=pb.ServerMsg.GAMEOVER, phase="gameover")
        print(f"[end] {who} win")
        time.sleep(1)
        for pid in list(self.players):
            self.srv.disconnect(pid)

    def _alive_names(self):
        with self.lock:
            return ", ".join(sorted(self.players[p] for p in self.alive))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--players", type=int, default=3,
                    help="minimum players before the game starts")
    args = ap.parse_args()

    srv = comms.CommsServer(host=args.host, port=args.port)
    game = Game(srv, args.players)
    srv.start()
    print(f"[moderator] listening on {args.host}:{args.port}")
    try:
        game.run()
    except KeyboardInterrupt:
        print("\n[moderator] interrupted")
    finally:
        srv.stop()


if __name__ == "__main__":
    main()
