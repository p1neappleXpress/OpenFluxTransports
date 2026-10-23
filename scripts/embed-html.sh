#!/usr/bin/env bash
# Turns a wizard page into the module its main.js requires.
#
#   scripts/embed-html.sh <id>          # transports/<id>/setup.html -> transports/<id>/setup_html.js
#   scripts/embed-html.sh --lib <name>  # lib/<name>.html            -> lib/<name>_html.js  (the shared wizard page)
#
# The page is written and previewed in a browser as .html; the module is the same text as a JS string
# (module.exports = "..."), which scriptbundle inlines into the script that gets signed. Commit both.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${1:-}" = "--lib" ]; then
  name=${2:?usage: scripts/embed-html.sh --lib <name>}
  src="lib/$name.html"
  out="lib/${name}_html.js"
else
  id=${1:?usage: scripts/embed-html.sh <id> | --lib <name>}
  src="transports/$id/setup.html"
  out="transports/$id/setup_html.js"
fi
[ -f "$src" ] || { echo "no such page: $src" >&2; exit 1; }
printf 'module.exports = %s;\n' "$(jq -Rs . < "$src")" > "$out"
