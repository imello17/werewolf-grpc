#!/usr/bin/env bash
# play_game.sh — plays one full 7-player mTLS game in REAL client.py terminals,
# so you can screenshot them. Run as root from anywhere:
#
#     sudo bash /opt/werewolf/test/play_game.sh
#
# Then, in another terminal, watch it:   sudo tmux attach -t ww
#   Ctrl-b 0      moderator          Ctrl-b 1..7   ali bob car dav eve fei gus
#   Ctrl-b d      detach (game keeps going)
#
# Window N = player(N-1), a separate Linux user running its own client.py.
# Moves are typed into each window with `tmux send-keys`, timed by waiting for
# the game's own prompts. Seed 19 with these names always gives:
#   wolves bob + dav, doctor car, jester gus, town ali/eve/fei.
# Script:
#   Night 1: wolves pick ali, doctor saves ali        -> "survived the night"
#   Day 1:   town lynches gus (the jester)            -> jester wins, GROUP warning
#   Night 2: wolves kill ali, doctor saves bob, ghost takes fei -> WOLVES win
#
# Needs: tmux, users player0..player6, certs/ from make_certs.sh.
set -euo pipefail

DIR="${WW_DIR:-/opt/werewolf}"
S=ww
NAMES=(ali bob car dav eve fei gus)
declare -A WIN=([mod]=0 [ali]=1 [bob]=2 [car]=3 [dav]=4 [eve]=5 [fei]=6 [gus]=7)

command -v tmux >/dev/null || { echo "install tmux: sudo apt install -y tmux"; exit 1; }
[[ -f "$DIR/certs/ca.crt" ]] || { echo "no $DIR/certs — run make_certs.sh first"; exit 1; }

if command -v sudo >/dev/null; then as_user() { echo "sudo -iu $1 bash -c '$2'"; }
else as_user() { echo "runuser -l $1 -c '$2'"; }; fi

say()   { tmux send-keys -t "$S:${WIN[$1]}" -l "$2"; tmux send-keys -t "$S:${WIN[$1]}" Enter; sleep 0.7; }
count() { tmux capture-pane -p -J -S - -t "$S:${WIN[$1]}" | grep -c -- "$2" || true; }
# wait_for WHO TEXT [N=1] [TIMEOUT=120]: block until TEXT has appeared N times in WHO's window
wait_for() {
  local who=$1 text=$2 n=${3:-1} t=${4:-120} i=0
  until (( $(count "$who" "$text") >= n )); do
    sleep 0.5; i=$((i+1))
    if (( i > t*2 )); then echo "timed out waiting for '$text' in $who"; exit 1; fi
  done
}

tmux kill-session -t "$S" 2>/dev/null || true
tmux new-session -d -s "$S" -x 150 -y 48 -n moderator
tmux send-keys -t "$S:0" "cd $DIR && python3 server.py --players 7 --tls certs --seed 19 --doctor-min 0 --jester-min 0" Enter
wait_for mod "listening" 1 20
echo "[play] moderator up — attach with: sudo tmux attach -t $S"

for i in "${!NAMES[@]}"; do
  n=${NAMES[$i]}
  tmux new-window -t "$S:$((i+1))" -n "$n"
  tmux send-keys -t "$S:$((i+1))" \
    "$(as_user "player$i" "cd $DIR && python3 client.py --tls certs --cert-name $n")" Enter
  sleep 0.6
done
wait_for gus "Welcome gus" 1 20
echo "[play] 7 players joined; lobby closes in ~20s"

# ---------------- NIGHT 1 ----------------
wait_for bob "Wolves, discuss" 1 60
say bob "who do we take tonight?"
say dav  "ali. she talks too much"
say bob "agreed, ali"
wait_for car "Doctor, /save" 1 30
say car "/save ali"
wait_for bob "Wolves, /vote" 1 30
say bob "/vote ali"
say dav "/vote ali"
echo "[play] night 1 done"

# ---------------- DAY 1 ----------------
wait_for ali "Discuss (" 1 60
say ali "something attacked me last night but I'm still here!"
say gus "that's obviously car. lynch me if I'm wrong ;)"
say eve "gus is acting really weird"
say bob "yeah, gus is sus"
say fei "fine, gus it is"
wait_for ali "lynch someone" 1 60
for p in ali bob car dav eve fei; do say "$p" "/vote gus"; done
say gus "/vote fei"
echo "[play] day 1 done (jester lynched)"

# ---------------- NIGHT 2 ----------------
wait_for gus "ghost rises" 1 60
say gus "/revenge fei"
wait_for car "Doctor, /save" 2 30
say car "/save bob"
wait_for bob "Wolves, discuss" 2 30
say dav "doctor can't save ali twice in a row"
say bob "ali again then"
wait_for bob "Wolves, /vote" 2 30
say bob "/vote ali"
say dav "/vote ali"

wait_for mod "\[end\]" 1 60
echo "[play] GAME OVER. Screenshot each window: Ctrl-b 0 (moderator), 1-7 (players)."
echo "[play] when finished: sudo tmux kill-session -t $S"
