# matcrawl

[![CI](https://github.com/peetzweg/matcrawl/actions/workflows/ci.yml/badge.svg)](https://github.com/peetzweg/matcrawl/actions/workflows/ci.yml)

Matrix archive CLI. Local-first.

`matcrawl` speaks the Matrix Client-Server API directly. It logs into your
account, decrypts E2EE rooms with your Megolm keys, pulls full message
history, and stores it in a searchable SQLite archive at
`~/.matcrawl/matcrawl.db`. Backup to a private git repo as age-encrypted
shards is optional.

Built on [`mautrix-go`](https://github.com/mautrix/go) and
[`openclaw/crawlkit`](https://github.com/openclaw/crawlkit) — the Matrix
sibling of [`sigcrawl`](https://github.com/peetzweg/sigcrawl) (Signal),
[`telecrawl`](https://github.com/openclaw/telecrawl) (Telegram),
[`wacli`](https://github.com/openclaw/wacli) (WhatsApp), and
[`slacrawl`](https://github.com/openclaw/slacrawl) (Slack). Same CLI
vocabulary across all of them.

## Install

### Homebrew (recommended)

    brew install peetzweg/tap/matcrawl

### Go install

    go install github.com/peetzweg/matcrawl/cmd/matcrawl@latest

`go install` drops the binary in `$GOPATH/bin`, which isn't on most
shells' `$PATH` by default. Add it once:

    export PATH="$HOME/go/bin:$PATH"

### Build from source

    git clone https://github.com/peetzweg/matcrawl
    cd matcrawl
    make build

`make build` uses `-tags goolm` to keep the binary CGO-free.

## First-time setup

### Authenticate

    matcrawl login

`login` prompts for your homeserver URL, user ID, device ID, and access
token. Find the latter two in Element → Settings → Help & About →
Advanced → Access Token. The token entry is hidden from your terminal
when stdin is a TTY.

Session lands at `~/.matcrawl/session.json` (mode 0o600). Run

    matcrawl doctor

to confirm the homeserver reaches and `/account/whoami` agrees with the
session.

### Decrypt E2EE rooms

matcrawl decrypts with your room keys. It never registers a device or
uploads keys.

**From your key backup (recommended).** If you set up recovery in
Element, pull every key your devices have backed up to the server:

    matcrawl keys backup-pull

It prompts for your recovery key (the `EsT…` key Element showed when you
set up recovery) or security phrase. The backup holds keys from all your
devices, including messages that arrived while a given Element install
was closed. It only reads from the server.

**From an Element export.** Alternatively, export room keys from Element
(Settings → Security & Privacy → Export E2E room keys → choose a
passphrase), then:

    matcrawl keys import ~/Downloads/element-keys.txt

An export only holds the keys that one Element install has, so it can
miss messages from times it wasn't running.

Both commands prompt for a secret, so run them in an interactive
terminal (or pipe the secret on stdin). Then decrypt what's stored:

    matcrawl keys retry

`keys retry` re-runs decryption on every stored message that is still
encrypted. New messages bring new keys, so after later syncs run
`keys backup-pull` and `keys retry` again. `matcrawl status` reports how
many messages are still undecrypted. Deleted messages, and messages
whose keys were never backed up, stay encrypted.

### Crawl

    matcrawl sync --backfill

The first run walks `/sync` for the room list, then `/rooms/{id}/messages?dir=b`
backwards per joined room until the homeserver runs out of history. Both
phases are resumable — `Ctrl-C` and re-run continues from the persisted
`next_batch` / `prev_batch` tokens. Subsequent `matcrawl sync` (without
`--backfill`) is incremental.

    matcrawl status

reports archive counts, decrypt-failure totals, and the latest sync
timestamp.

## Read

    matcrawl rooms --limit 20
    matcrawl rooms --encrypted-only
    matcrawl members --room '!abc:server.example'
    matcrawl messages --room '!abc:server.example' --after 2026-01-01
    matcrawl messages --media
    matcrawl messages --space '!space:server.example' --sender @alice:server.example
    matcrawl search "<query>" --limit 20
    matcrawl spaces

All commands accept `--json`, before or after the subcommand, for
machine-readable output. `matcrawl metadata` returns the crawlkit
control manifest for agent integrations.

## What's archived

- **Rooms**: name, alias, topic, avatar, encryption, DM flag (from
  `m.direct`), room type and version, creator, power levels, upgrades
  (predecessor/replacement room) and the spaces each room belongs to. Rooms
  without a name get one from their members, as Element does
  (`display_name`).
- **Messages**: text, formatting, attachments, stickers, polls, threads and
  replies, with the sender's display name and the room name. Edited messages
  show their current text plus `original_body` and the full edit history.
  Search matches the current text.
- **Reactions**: aggregated per emoji with who reacted.
- **Deletions**: deleted messages are flagged with who deleted them and why.
  Content captured before the deletion is kept.

`matcrawl sync` fills gaps when a busy room had more activity than one sync
returns. `matcrawl sync --backfill` walks each room's history back to its
creation. After an upgrade that captures new kinds of events, it re-walks
each room once, and Ctrl-C resumes where it stopped.

## Backup (optional)

    matcrawl backup init --repo ~/Projects/backup-matcrawl --remote git@github.com:you/backup-matcrawl.git
    matcrawl backup push

The repository contains only age-encrypted `.jsonl.gz.age` shards plus a
manifest — rooms in `data/rooms.jsonl.gz.age`, members in
`data/members.jsonl.gz.age`, messages bucketed by year/month under
`data/messages/<year>/<month>.jsonl.gz.age`. Decrypt via

    matcrawl backup pull --repo ~/Projects/backup-matcrawl

against a clean machine.

## E2EE caveats (v0)

matcrawl doesn't receive room keys live the way Element does. It
decrypts with the keys you pull from the backup or import, so run
`matcrawl keys backup-pull` and `matcrawl keys retry` after syncing to
read new messages. The v1 cross-signing / key-forwarding flow will
eliminate this step.

## Not supported (v0)

- **Matrix Spaces** hierarchy preservation — child rooms are archived as
  ordinary rooms.
- **Media blob download** from `mxc://` URIs — paths recorded in
  `attachments_json`, the actual blobs aren't fetched. Planned for v1.
- **iOS / Android Element export** — no comparable format exists.
- **Sending messages** — matcrawl is read-only. Use
  [matrix-commander](https://github.com/8go/matrix-commander) for that.
- **Multiple accounts** — v0 supports one account at a time.

## License

MIT
