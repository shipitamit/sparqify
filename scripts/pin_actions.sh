#!/usr/bin/env bash
# Replace `uses: owner/repo@tag` in workflows and action.yml with the full commit SHA (keeps the tag as a comment).
# Needs the GitHub CLI (`gh auth login`). Re-run it when you bump a version. Review the diff before committing.
# Works with GNU sed (Linux) and BSD sed (macOS): `-i.bak -E` is the portable spelling, and the backups are removed again.
set -euo pipefail
cd "$(dirname "$0")/.."
files=$(ls .github/workflows/*.yml action.yml)
grep -hoE 'uses: [A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[^ #]+' $files | sort -u | while read -r _ ref; do
  repo=${ref%@*}; tag=${ref#*@}
  [[ $tag =~ ^[0-9a-f]{40}$ ]] && continue
  sha=$(gh api "repos/$(echo "$repo" | cut -d/ -f1,2)/commits/$tag" --jq .sha)
  [[ $sha =~ ^[0-9a-f]{40}$ ]] || { echo "could not resolve $ref" >&2; exit 1; }
  sed -i.bak -E "s|uses: ${repo}@${tag}( *#.*)?$|uses: ${repo}@${sha} # ${tag}|" $files
  for f in $files; do rm -f "$f.bak"; done
  grep -q "uses: ${repo}@${sha}" $files || { echo "pinned $ref to $sha but no file changed: check the sed in this script" >&2; exit 1; }
  echo "$ref -> $sha"
done
