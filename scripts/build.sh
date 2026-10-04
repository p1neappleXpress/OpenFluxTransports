#!/usr/bin/env bash
# Builds one transport into a signed .flux package.
#
#   scripts/build.sh <id> <signing.key> [out-dir]      # -> <out-dir>/<id>-<version>.flux
#
# The entry (transports/<id>/main.js) is bundled with its shared lib/ modules by
# the OpenFlux scriptbundle, then packed and signed with scriptsign together
# with transports/<id>/manifest.json. scriptsign refuses a manifest that
# disagrees with the script's own info() (name, version).
set -euo pipefail
id=${1:?usage: scripts/build.sh <id> <signing.key> [out-dir]}
key=${2:?usage: scripts/build.sh <id> <signing.key> [out-dir]}
cd "$(dirname "$0")/.."
out=${3:-dist}
ref=${OPENFLUX_REF:-nightly}   # the OpenFlux core the tools come from

[ -f "transports/$id/main.js" ] || { echo "no such transport: $id" >&2; exit 1; }
bin="$(go env GOPATH)/bin"
if [ ! -x "$bin/scriptbundle" ] || [ ! -x "$bin/scriptsign" ]; then
  go install "github.com/p1neappleXpress/OpenFlux/transport/script/cmd/scriptbundle@$ref" \
             "github.com/p1neappleXpress/OpenFlux/transport/script/cmd/scriptsign@$ref"
fi

version=$(jq -r .version "transports/$id/manifest.json")
mkdir -p "$out/build"
"$bin/scriptbundle" -entry "transports/$id/main.js" -out "$out/build/$id.js"
"$bin/scriptsign" pack "$key" "transports/$id/manifest.json" "$out/build/$id.js" "$out/$id-$version.flux"
