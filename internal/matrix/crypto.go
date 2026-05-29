package matrix

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.mau.fi/util/dbutil"
	_ "modernc.org/sqlite"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
)

// pickleKeyBytes is the size of the random key used to encrypt the local
// crypto store on disk. 32 bytes matches mautrix's recommended length.
const pickleKeyBytes = 32

// CryptoPaths bundles the on-disk locations matcrawl uses for the crypto
// subsystem. Computed from the configured --db path so that overrides keep
// the session, crypto DB, and pickle key consistent.
type CryptoPaths struct {
	CryptoDB  string
	PickleKey string
}

func DefaultCryptoPaths(dbPath string) CryptoPaths {
	dir := filepath.Dir(dbPath)
	return CryptoPaths{
		CryptoDB:  filepath.Join(dir, "crypto.db"),
		PickleKey: filepath.Join(dir, "pickle.key"),
	}
}

// EnsurePickleKey reads the pickle key file or generates a fresh one with
// 0o600 perms in a 0o700 dir. The key encrypts the local crypto store so it
// can't be lifted from disk without it.
func EnsurePickleKey(path string) ([]byte, error) {
	if data, err := os.ReadFile(path); err == nil {
		if len(data) != pickleKeyBytes {
			return nil, fmt.Errorf("pickle key at %s has wrong length %d (want %d)", path, len(data), pickleKeyBytes)
		}
		return data, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir pickle dir: %w", err)
	}
	key := make([]byte, pickleKeyBytes)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, key, 0o600); err != nil {
		return nil, err
	}
	return key, nil
}

// OpenCrypto wires up a cryptohelper backed by a modernc.org/sqlite database
// (pure-Go, no libsqlite or CGO). The cryptohelper bundles a SQLStateStore
// and SQLCryptoStore; Init upgrades both schemas and loads the Olm account.
//
// The returned helper holds the database open until Close is called.
func OpenCrypto(ctx context.Context, client *mautrix.Client, paths CryptoPaths) (*cryptohelper.CryptoHelper, error) {
	pickleKey, err := EnsurePickleKey(paths.PickleKey)
	if err != nil {
		return nil, fmt.Errorf("pickle key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.CryptoDB), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir crypto db dir: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)", paths.CryptoDB)
	rawDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open crypto db: %w", err)
	}
	// Single connection is enough for the crawl workload and avoids
	// SQLite-level locking surprises across goroutines.
	rawDB.SetMaxOpenConns(1)
	if err := rawDB.PingContext(ctx); err != nil {
		_ = rawDB.Close()
		return nil, fmt.Errorf("ping crypto db: %w", err)
	}
	db, err := dbutil.NewWithDB(rawDB, "sqlite")
	if err != nil {
		_ = rawDB.Close()
		return nil, fmt.Errorf("wrap crypto db: %w", err)
	}
	helper, err := cryptohelper.NewCryptoHelper(client, pickleKey, db)
	if err != nil {
		_ = rawDB.Close()
		return nil, fmt.Errorf("new crypto helper: %w", err)
	}
	if err := helper.Init(ctx); err != nil {
		_ = rawDB.Close()
		return nil, fmt.Errorf("init crypto helper: %w", err)
	}
	return helper, nil
}

// ImportKeyExport reads an Element room-keys .txt export from disk and feeds
// its Megolm sessions into the OlmMachine. Returns (imported, total) counts.
func ImportKeyExport(ctx context.Context, helper *cryptohelper.CryptoHelper, path, passphrase string) (int, int, error) {
	machine := helperMachine(helper)
	if machine == nil {
		return 0, 0, errors.New("crypto helper has no OlmMachine — was Init called?")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, fmt.Errorf("read key export: %w", err)
	}
	return machine.ImportKeys(ctx, passphrase, data)
}

// DecryptResult summarises a single decryption attempt.
type DecryptResult struct {
	Body          string
	FormattedBody string
	MsgType       string
	Status        string
	ErrorString   string
}

// DecryptStored attempts to decrypt a previously stored m.room.encrypted
// event by re-parsing its raw JSON and running it through the OlmMachine.
// Returns a result that the caller can fold back into the store row.
func DecryptStored(ctx context.Context, helper *cryptohelper.CryptoHelper, raw string) DecryptResult {
	machine := helperMachine(helper)
	if machine == nil {
		return DecryptResult{Status: "failed", ErrorString: "no OlmMachine"}
	}
	var evt event.Event
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return DecryptResult{Status: "failed", ErrorString: "raw_json unparseable: " + err.Error()}
	}
	evt.Type = event.EventEncrypted
	decrypted, err := machine.DecryptMegolmEvent(ctx, &evt)
	if err != nil {
		// Surface "missing_keys" specifically when the OlmMachine can't find a session;
		// any other error is a real failure to flag in `status`.
		if errors.Is(err, crypto.NoSessionFound) {
			return DecryptResult{Status: "missing_keys", ErrorString: err.Error()}
		}
		return DecryptResult{Status: "failed", ErrorString: err.Error()}
	}

	res := DecryptResult{Status: "ok"}
	if err := decrypted.Content.ParseRaw(decrypted.Type); err == nil {
		if msg := decrypted.Content.AsMessage(); msg != nil {
			res.MsgType = string(msg.MsgType)
			res.Body = msg.Body
			res.FormattedBody = msg.FormattedBody
		}
	}
	if res.MsgType == "" {
		res.MsgType = string(decrypted.Type.Type)
	}
	return res
}

// helperMachine returns the OlmMachine that the cryptohelper manages. It's a
// public field on cryptohelper.CryptoHelper, but we route through this
// indirection so the rest of matcrawl doesn't take a hard dependency on the
// helper's internals.
func helperMachine(helper *cryptohelper.CryptoHelper) *crypto.OlmMachine {
	if helper == nil {
		return nil
	}
	return helper.Machine()
}
