# OpenFlux transports

The official [OpenFlux](https://github.com/p1neappleXpress/OpenFlux) transports
as signed, versioned JS packages, one directory each. The apps ship a snapshot
of them, update them in place (Transports tab) and list them in a catalog. This
is the source and the release pipeline; to write a transport of your own, fork
[OpenFluxTransportTemplate](https://github.com/p1neappleXpress/OpenFluxTransportTemplate).

*[Читать на русском](README.ru.md)*

| transport | carries packets in |
|---|---|
| `yandex` | a Yandex Docs co-editing session (cursor field) |
| `vyandex` | a Yandex Volga document session |
| `boards` | text objects on a Yandex Boards whiteboard |
| `mailru` | a Mail.ru public document session (cursor field) |
| `cupsonline` | cursor integers in cups.online rooms (an exit can create its rooms) |
| `oneme-iceinject` | MAX call signaling (the path the native transport uses) |
| `oneme-webrtc` | a MAX WebRTC data channel |
| `mtslink` | cursor position updates on an MTS-Link whiteboard (by trader52) |
| `bitrix` | cursor position updates on a Bitrix24 Flipchart whiteboard (by trader52) |

## Layout

```
transports/<id>/main.js         the transport (starts a bundle: it may require("../../lib/..."))
transports/<id>/manifest.json   id, name, version, wire, api, update (signed with the package)
lib/                            modules shared by several transports (captcha, MAX client)
index.json                      the catalog the apps show: one entry per transport
scripts/build.sh                bundle + pack + sign one transport
```

`name` and `version` in `manifest.json` must equal what the script's own
`info()` says; `scriptsign pack` refuses a mismatch. `wire` is the generation of
the transport's wire format: client and node both run a transport, so a new
`wire` is a breaking change that apps hold back until the user confirms.

## Releasing

Push a tag `<id>-v<version>`:

```bash
# bump "version" in transports/yandex/manifest.json AND info().version in main.js, commit, then:
git tag yandex-v1.2.0 && git push --tags      # channel "stable"
git tag yandex-v1.3.0-beta.1 && git push --tags   # channel "nightly" (a pre-release)
```

CI builds only that transport, signs it with the repository secret `SIGNING_KEY`
(the OpenFlux release key), publishes `<id>-<version>.flux` as a release asset and
records it in `<id>/update.json` on the `updates` branch, which is what installed
apps poll. A stable release never drops the nightly entry. The check workflow
builds every transport with a throwaway key on each push.

### Without a signing secret

The release workflow needs the repository secret `SIGNING_KEY`. Without it
(the OpenFlux root key is deliberately not stored in GitHub) a release is made by
the key holder: build with `scripts/build.sh <id> <key>`, create the GitHub release
with the `.flux` asset, and write `<id>/update.json` on the `updates` branch with
`scriptsign index` (the workflow's last step shows the exact command). The tag
still triggers the workflow; it notices there is no key and does nothing.

## Keys

Apps trust a transport as "official" if it verifies under one of the keys in the
core's `officialKeys` list (`transport/script/trust.go`). Rotating the release key
is two core releases: the first adds the new public key to that list, the second
(once most apps have it) switches `SIGNING_KEY`. Official updates signed by another
official key are accepted and the install is re-pinned to it. A leaked key is
retired by removing it from the list in a core release.

## Tests

The transports are checked against the native ones (byte for byte where the wire
is pure, through a fake server where it is not) in the OpenFlux repository:
`transport/script/parity_test.go`, `interop_cups_test.go`,
`docs/plans/2026-10-04-native-js-parity.md`. CI here builds the tools from the
core's `nightly` branch; set the repository variable `OPENFLUX_REF` to pin another
branch or tag.
