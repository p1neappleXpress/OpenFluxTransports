#!/usr/bin/env bash
# Turns a transport's wizard page into the module its main.js requires.
#
#   scripts/embed-html.sh <id>      # transports/<id>/setup.html -> transports/<id>/setup_html.js
#
# setup.html is the page as it is written and previewed in a browser; setup_html.js is the same text as a JS
# string (module.exports = "..."), which scriptbundle inlines into the script that gets signed. Commit both.
set -euo pipefail
id=${1:?usage: scripts/embed-html.sh <id>}
cd "$(dirname "$0")/.."
src="transports/$id/setup.html"
[ -f "$src" ] || { echo "no such page: $src" >&2; exit 1; }
printf 'module.exports = %s;\n' "$(jq -Rs . < "$src")" > "transports/$id/setup_html.js"
