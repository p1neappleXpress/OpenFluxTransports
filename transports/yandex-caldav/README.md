# yandex-caldav

Packets ride events in a private calendar on `caldav.yandex.ru`. The transport
is an HTTP/CalDAV client: both ends share one Yandex account, one calendar and
one channel name, and exchange length-prefixed frames in the `DESCRIPTION` of
two events (one per direction). The exit creates the calendar on first run.

## Setup

1. Create an app password of type **Календарь**: Яндекс ID → Безопасность →
   Пароли приложений → Календарь. A regular account password does not work for
   CalDAV; a password of another type (Почта, Контакты) does not either.
2. On both ends set:
   - **Канал** (the profile's URL field) — the same short name, e.g. `main`;
   - **Почта Яндекса** — the full address (`ilyastaleb@yandex.ru`);
   - **Пароль приложения** — the Calendar app password;
   - **Календарь** — `OpenFlux` by default. The exit creates it if missing;
     a client waits until it appears. A full path (`/calendars/.../`) skips
     discovery.
3. The exit starts first. It needs CalDAV write access to the calendar; the
   client only needs the same account (it writes and reads its own events).

Both ends must use the same account: there is no sharing link, because a
Yandex calendar is private. Treat the app password as part of the profile.

## How it works

```
<calendar>/ofx-<channel>-c2s-0.ics   written by the client, read by the exit
<calendar>/ofx-<channel>-s2c-0.ics   written by the exit, read by the client
```

Every event's `DESCRIPTION` is

```
OFX1-<ack-epoch>.<ack-seq>-<base64 frames>
```

where the ack says how far this end has read the peer's frames. A frame is
`epoch(4) seq(4) ts(4) len(2) payload`; a PUT carries all unacknowledged
frames that fit in one event (640 KiB), so a lost PUT or an event overwritten
before the peer polled it is carried again by the next one. The reader
acknowledges frames it has consumed, which frees the writer's ring.

The `slots` setting (default 1) can spread the channel over several event
pairs. It is an experiment: a single stream is fastest on one slot, because
spreading its packets reorders them; the core's batching layer also hands the
transport opaque batches, not bare IP packets, so the transport cannot pin a
flow to a slot reliably. One slot is the supported configuration.

Events live in 2030 (`DTSTART:20300101`), are `TRANSP:TRANSPARENT` and
`CLASS:PRIVATE`, so they stay out of the current calendar view and do not
block time. Yandex rewrites events on PUT and drops unknown `X-` properties
(checked 2026-10-31), which is why the payload lives in `DESCRIPTION`.

## Numbers measured on a live account (2026-10-31)

| what | result |
|---|---|
| discovery + calendar creation | 3 requests, ~2 s |
| scripttest pair (exit + client) | both directions delivered, ~1.5 s |
| 200 × 1 KiB burst | 201/201 packets, ~13 s |
| single 640 KiB window PUT | 0.9–1.3 s (carrier burst ~500–700 KB/s) |
| real SOCKS5 tunnel, `https://example.com` | HTTP 200, ~6 s end to end |
| real SOCKS5 tunnel, 5 MiB download | ~130 KB/s |
| real SOCKS5 tunnel, 10 MiB download | **150 KB/s** (70 s) |
| 8 parallel 1 MiB downloads | ~140–150 KB/s aggregate |
| CalDAV request ceiling | ~58 PROPFIND/s (16 threads), no 429s |
| sustained small-event GET | ~330 KB/s per account |

## Several accounts

A lane named `a2-…` uses the second entry of the `accounts` setting, `a1-…`
the first; a lane without the prefix uses the profile's main email/password.
The value is `email:app-password|email:app-password` (one per prefix). This is
how two Yandex accounts can feed one striped group:

```
URL = a1-main-01,a1-main-02,a2-main-01,a2-main-02
accounts = first@yandex.ru:app-password-1|second@yandex.ru:app-password-2
```

Each account has its own `OpenFlux` calendar (the exit creates it on first
run). Per-account CalDAV request and bandwidth limits still apply; two
accounts only add up if the lanes are split between them.

## Lanes

The core can stripe several script lanes into one transport: give both ends
the same comma-separated list, e.g. `URL = lane-a,lane-b,lane-c,lane-d`.
Each lane is its own channel (its own pair of events) in the same calendar.
The core's `transport/registry/group.go` must allow `script` groups and needs
the CalDAV stripe profile (long RTOs, no pacing):

```go
case "script":
    return transport.StripeOptions{
        PieceBytes: 32 << 10, DeadAfter: 30 * time.Second, NoDups: true,
        NoPacing: true, InitRTO: 10 * time.Second, MinRTO: 8 * time.Second,
        MaxRTO: 30 * time.Second, HoleDeadline: 30 * time.Second,
        AckInterval: 250 * time.Millisecond, LaneWindow: true,
        CodelTarget: 1500 * time.Millisecond, CodelInterval: 3000 * time.Millisecond,
    }
```

With the stock Mail.ru stripe profile the lanes answer in 2–4 s while the
stripe's RTO is 2–3 s: on 2026-10-31 it resent **644 of 715 pieces**, the
tunnel's TCP counted 1969 retransmissions, and 8 streams ran at 125 KB/s.
With the profile above the same test shows `rtx=0`, `late=0`, `codelDrop=0`.

Measured with 8 parallel 1 MiB downloads (2026-10-31):

| lanes | time | aggregate |
|---|---|---|
| 4 | 33 s | 254 KB/s |
| 6 | 32 s | 262 KB/s |
| 6, 16 streams | 104 s | 159 KB/s (lane queues full) |

Full matrix (8 × 1 MiB flows, 20 s, one account, KB/s; a 0 is a run that
failed to come up, not a measurement):

| lanes \ slots | 1 | 2 | 3 |
|---|---|---|---|
| 1 | 199 | 89 | 87 |
| 2 | 19 | 381 | **452** |
| 4 | 308 | 396 | 346 |
| 6 | 433 | 421 | 0 |
| 8 | 340 | 0 | **466** |

Best stable: 2 lanes × 3 slots or 8 × 3, **~450–470 KB/s** (3.6–3.8 Mbit/s).
The raw CalDAV carrier on the same link does ~2.4 MB/s and the internet link
7 MB/s, so the ceiling here is the tunnel's TCP over the carrier's RTT, not
Yandex and not the stripe. Two accounts do not add: 2+2 lanes at one slot
measured 262 KB/s, so the app's TCP window, not the carrier, is the limit.
Reaching 8 Mbit/s needs the core's TCP tuned, or several independent tunnels.
So lanes double the single-lane 150 KB/s but do not reach 500: what is left is
the per-lane drain (the script's window and the account's request budget) and
the tunnel's TCP, not the stripe.

So the carrier itself moves 350 KB/s for the tunnel and bursts to ~700 KB/s,
while the end-to-end TCP sees **150 KB/s**. That is the ceiling of this design:
the tunnel's TCP RTT over the calendar is ~2.5 s, and TCP throughput is
window/RTT. 500 KB/s would need a ~1.25 MB receive window or a ~1 s RTT.
Raising the poll/post rate does not change that: the server already sustains
~60 requests/s, and 500× the current rate would be thousands of requests per
second — throttled, not faster. More parallel tunnels (separate profiles)
do add up: four independent 150 KB/s tunnels give ~600 KB/s.

## Yandex specifics

- Principal: `/principals/users/<email>/`, home `/calendars/<email>/`.
  Yandex answers 404 to the `%40`-encoded form it puts in its own hrefs, so
  the transport decodes `%40` back to `@` before following an href.
- `MKCALENDAR` works but the server renames the collection to `events-<id>`;
  the transport creates it, then finds it by `displayname`.
- **An event is stored under its `UID`, not under the PUT path.** The UID must
  therefore equal the resource's base name; otherwise a successful PUT lands
  under a different name and the GET 404s. The transport's UIDs and paths
  match (`ofx-<channel>-c2s-0.ics`).
- `If-None-Match` on GET is ignored (always 200), so the poll uses
  `PROPFIND Depth: 0` for `getetag` and fetches the event only when the etag
  changes.
- A calendar is created automatically by the exit (`calendar` setting,
  `OpenFlux` by default). Deleting it in `calendar.yandex.ru` removes every
  trace of the channel.
