#!/usr/bin/env bash
# Pin the base images in the Dockerfiles to a digest: `FROM python:3.12-slim` -> `FROM python:3.12-slim@sha256:...` (the tag stays, so it is readable and
# Dependabot can update it). Needs Docker with buildx and network access to the registry. Re-run it to refresh the pins; review the diff before committing.
set -euo pipefail
cd "$(dirname "$0")/.."
for f in Dockerfile deploy/sandbox/Dockerfile; do
  [ -f "$f" ] || continue
  grep -oE '^FROM +[^ ]+' "$f" | awk '{print $2}' | sort -u | while read -r img; do
    base=${img%%@*}                                              # drop an old digest, keep image:tag
    [[ $base == scratch ]] && continue
    digest=$(docker buildx imagetools inspect "$base" --format '{{.Manifest.Digest}}')
    [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "could not resolve $base" >&2; exit 1; }
    sed -i.bak -E "s|^(FROM +)${base//./\\.}(@sha256:[0-9a-f]{64})?( .*)?$|\1${base}@${digest}\3|" "$f" && rm -f "$f.bak"
    echo "$f: $base -> $digest"
  done
done
