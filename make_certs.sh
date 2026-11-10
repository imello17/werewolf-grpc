#!/usr/bin/env bash
# make_certs.sh — a tiny private CA for Werewolf mTLS.
#
#   ./make_certs.sh ca                          # once: certs/ca.key + certs/ca.crt
#   ./make_certs.sh server [HOST_OR_IP ...]     # certs/server.{key,crt}
#                                               # (localhost + 127.0.0.1 always included)
#   ./make_certs.sh player NAME [LINUX_USER]    # certs/NAME.{key,crt}, CN=NAME
#
# The player's certificate CN *is* their in-game name. With LINUX_USER given
# (run as root), the private key is chowned to that user and chmod 600, so only
# that Linux account can read it — and therefore only that account can play
# as NAME. This ties game identity to OS identity.
#
# ca.key must stay with whoever runs the game (the moderator); anyone holding
# it can mint certificates for any name.
set -euo pipefail

DIR="${CERT_DIR:-certs}"
DAYS=365
mkdir -p "$DIR"
cd "$DIR"

need_ca() {
  [[ -f ca.key && -f ca.crt ]] || { echo "run '$0 ca' first" >&2; exit 1; }
}

case "${1:-}" in
  ca)
    openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" \
      -keyout ca.key -out ca.crt -subj "/CN=Werewolf Test CA" 2>/dev/null
    chmod 600 ca.key; chmod 644 ca.crt
    echo "created $DIR/ca.key (keep private) and $DIR/ca.crt (give to everyone)"
    ;;

  server)
    need_ca; shift
    san="DNS:localhost,IP:127.0.0.1"
    for h in "$@"; do
      if [[ "$h" =~ ^[0-9.]+$ ]]; then san+=",IP:$h"; else san+=",DNS:$h"; fi
    done
    openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
      -subj "/CN=werewolf-server" 2>/dev/null
    openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
      -days "$DAYS" -out server.crt \
      -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth\n" "$san") \
      2>/dev/null
    rm -f server.csr; chmod 600 server.key
    echo "created $DIR/server.{key,crt}  SAN: $san"
    ;;

  player)
    need_ca
    name="${2:?usage: $0 player NAME [LINUX_USER]}"
    [[ "$name" =~ ^[A-Za-z0-9_-]{1,16}$ ]] || { echo "bad name: $name" >&2; exit 1; }
    openssl req -newkey rsa:2048 -nodes -keyout "$name.key" -out "$name.csr" \
      -subj "/CN=$name" 2>/dev/null
    openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
      -days "$DAYS" -out "$name.crt" \
      -extfile <(printf "extendedKeyUsage=clientAuth\n") 2>/dev/null
    rm -f "$name.csr"; chmod 600 "$name.key"; chmod 644 "$name.crt"
    if [[ -n "${3:-}" ]]; then
      chown "$3" "$name.key" "$name.crt"
      echo "created $DIR/$name.{key,crt}  CN=$name  owner=$3 (key mode 600)"
    else
      echo "created $DIR/$name.{key,crt}  CN=$name"
    fi
    ;;

  *)
    sed -n '2,15p' "$0"; exit 1 ;;
esac
