#!/bin/sh
set -e

# Injecte UMAMI_WEBSITE_ID dans l'HTML servi. Si la variable est vide,
# le tag <script> reste présent avec data-website-id="" — sans casser la page
# mais sans tracker.
: "${UMAMI_WEBSITE_ID:=}"
: "${DASHBOARD_URL:=http://localhost:3000/dashboard/2}"
envsubst '${UMAMI_WEBSITE_ID} ${DASHBOARD_URL}' \
  < /usr/share/nginx/html/index.html.template \
  > /usr/share/nginx/html/index.html

exec "$@"
