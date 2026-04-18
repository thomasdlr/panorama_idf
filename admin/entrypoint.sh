#!/bin/sh
set -e

: "${ADMIN_USER:=admin}"
if [ -z "${ADMIN_PASSWORD:-}" ]; then
  echo "ERROR: ADMIN_PASSWORD must be set for the admin service" >&2
  exit 1
fi

htpasswd -Bbc /etc/nginx/.htpasswd "$ADMIN_USER" "$ADMIN_PASSWORD" > /dev/null
echo "Admin basic auth configured for user: $ADMIN_USER"

exec "$@"
