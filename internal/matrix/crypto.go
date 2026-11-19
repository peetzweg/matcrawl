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

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	_ "modernc.org/sqlite"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/sqlstatestore"

	"github.com/peetzweg/matcrawl/internal/store"
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

// OpenMachine opens the local crypto store and loads its Olm account without
// publishing any device keys.
//
// matcrawl reuses the device ID of the Element session it was logged in with,
// and that device's identity keys already live on the server under Element's
// account. cryptohelper.Init would try to publish matcrawl's own account under
// the same device ID and refuses ("olm account is not marked as shared, but
// there are keys on the server"), which is correct, since overwriting them
// would break Element. Importing Megolm sessions and decrypting stored events
// need neither a published device nor a sync loop, so this does what Init does
// up to loading the account and stops there.
//
// The returned close function releases the crypto database.
func OpenMachine(ctx context.Context, client *mautrix.Client, paths CryptoPaths) (*crypto.OlmMachine, func(), error) {
	if client.UserID == "" || client.DeviceID == "" {
		return nil, nil, errors.New("the client must be logged in")
	}
	pickleKey, err := EnsurePickleKey(paths.PickleKey)
	if err != nil {
		return nil, nil, fmt.Errorf("pickle key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.CryptoDB), 0o700); err != nil {
		return nil, nil, fmt.Errorf("mkdir crypto db dir: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)", paths.CryptoDB)
	rawDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open crypto db: %w", err)
	}
	// Single connection is enough for the crawl workload and avoids
	// SQLite-level locking surprises across goroutines.
	rawDB.SetMaxOpenConns(1)
	closeFn := func() { _ = rawDB.Close() }
	if err := rawDB.PingContext(ctx); err != nil {
		closeFn()
		return nil, nil, fmt.Errorf("ping crypto db: %w", err)
	}
	db, err := dbutil.NewWithDB(rawDB, "sqlite")
	if err != nil {
		closeFn()
		return nil, nil, fmt.Errorf("wrap crypto db: %w", err)
	}

	log := zerolog.Nop()
	stateStore := sqlstatestore.NewSQLStateStore(db, dbutil.ZeroLogger(log), false)
	if err := stateStore.Upgrade(ctx); err != nil {
		closeFn()
		return nil, nil, fmt.Errorf("upgrade state store: %w", err)
	}
	cryptoStore := crypto.NewSQLCryptoStore(db, dbutil.ZeroLogger(log), "", client.DeviceID, pickleKey)
	if err := cryptoStore.DB.Upgrade(ctx); err != nil {
		closeFn()
		return nil, nil, fmt.Errorf("upgrade crypto store: %w", err)
	}
	stored, err := cryptoStore.FindDeviceID(ctx)
	if err != nil {
		closeFn()
		return nil, nil, fmt.Errorf("find stored device ID: %w", err)
	}
	if stored != "" && stored != client.DeviceID {
		closeFn()
		return nil, nil, fmt.Errorf("crypto store belongs to device %s but the session is device %s", stored, client.DeviceID)
	}

	machine := crypto.NewOlmMachine(client, &log, cryptoStore, stateStore)
	// Decrypting archived history needs neither sender device lookups (a
	// /keys/query per unknown device) nor replay bookkeeping (a crypto DB
	// write per message).
	machine.DisableDecryptKeyFetching = true
	machine.DisableRatchetTracking = true
	if err := machine.Load(ctx); err != nil {
		closeFn()
		return nil, nil, fmt.Errorf("load olm account: %w", err)
	}
	return machine, closeFn, nil
}

// ImportKeyExport reads an Element room-keys .txt export from disk and feeds
// its Megolm sessions into the OlmMachine. Returns (imported, total) counts.
func ImportKeyExport(ctx context.Context, machine *crypto.OlmMachine, path, passphrase string) (int, int, error) {
	if machine == nil {
		return 0, 0, errors.New("no OlmMachine")
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
	// Event is the decrypted event when Status is "ok".
	Event *event.Event
}

// DecryptStored attempts to decrypt a previously stored m.room.encrypted
// event by re-parsing its raw JSON and running it through the OlmMachine.
// roomID is the room the row belongs to; it fills in for events stored from
// /sync before room_id was added to their raw JSON.
// Returns a result that the caller can fold back into the store row.
func DecryptStored(ctx context.Context, machine *crypto.OlmMachine, roomID id.RoomID, raw string) DecryptResult {
	if machine == nil {
		return DecryptResult{Status: "failed", ErrorString: "no OlmMachine"}
	}
	var evt event.Event
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return DecryptResult{Status: "failed", ErrorString: "raw_json unparseable: " + err.Error()}
	}
	evt.Type = event.EventEncrypted
	if evt.RoomID == "" {
		evt.RoomID = roomID
	}
	if err := parseContent(&evt, event.EventEncrypted); err != nil {
		return DecryptResult{Status: "failed", ErrorString: "encrypted content unparseable: " + err.Error()}
	}
	decrypted, err := machine.DecryptMegolmEvent(ctx, &evt)
	if err != nil {
		// Surface "missing_keys" specifically when the OlmMachine can't find a session;
		// any other error is a real failure to flag in `status`.
		if errors.Is(err, crypto.NoSessionFound) {
			return DecryptResult{Status: "missing_keys", ErrorString: err.Error()}
		}
		return DecryptResult{Status: "failed", ErrorString: err.Error()}
	}

	res := DecryptResult{Status: "ok", Event: decrypted}
	if err := parseContent(decrypted, decrypted.Type); err == nil {
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

// DecryptedBatch classifies a successfully decrypted event the same way sync
// does, so encrypted edits and reactions land in their own tables. stored is
// the encrypted placeholder row; its raw ciphertext is kept for re-decryption.
// If the event turns out not to be a message, the placeholder is dropped.
func DecryptedBatch(stored store.Message, decrypted *event.Event) store.Batch {
	var b store.Batch
	appendEvent(&b, id.RoomID(stored.RoomID), decrypted)
	for i := range b.Messages {
		b.Messages[i].WasEncrypted = true
		b.Messages[i].DecryptStatus = "ok"
		b.Messages[i].RawJSON = stored.RawJSON
	}
	for i := range b.Edits {
		b.Edits[i].WasEncrypted = true
	}
	for i := range b.Reactions {
		b.Reactions[i].WasEncrypted = true
	}
	if len(b.Messages) == 0 {
		b.DropMessages = append(b.DropMessages, store.EventRef{RoomID: stored.RoomID, EventID: stored.EventID})
	}
	return b
}
