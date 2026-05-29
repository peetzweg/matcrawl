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

Export your room keys from Element (Settings → Security & Privacy →
Export E2E room keys → choose a passphrase), then:

    matcrawl keys import ~/Downloads/element-keys.txt

`matcrawl keys retry` re-runs decryption against every stored
ciphertext event whose Megolm session you just imported. After a future
sync, run it again if `matcrawl status` still reports
`decrypt_failures > 0`.

> Server-side key backup pull (the 4S recovery-key path) is planned for a
> follow-up release. For v0.1.0 the `.txt` export is the supported entry.

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
    matcrawl search "<query>"

All commands accept `--json` (must come before the subcommand) for
machine-readable output. `matcrawl metadata` returns the crawlkit
control manifest for agent integrations.

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

matcrawl decrypts encrypted rooms using the keys you import via
`matcrawl keys import`. If you join a new encrypted room *after*
exporting keys, re-run the export from Element and call
`matcrawl keys import keys.txt` again. The v1 cross-signing /
key-forwarding flow will eliminate this step.

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
