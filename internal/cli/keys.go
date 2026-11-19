package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/id"

	"github.com/peetzweg/matcrawl/internal/matrix"
	"github.com/peetzweg/matcrawl/internal/store"
)

func (r *runtime) runKeys(args []string) error {
	if len(args) == 0 {
		return usageErr(errors.New("keys needs subcommand: import, retry, backup-pull"))
	}
	switch args[0] {
	case "import":
		return r.keysImport(args[1:])
	case "retry":
		return r.keysRetry(args[1:])
	case "backup-pull":
		return r.keysBackupPull(args[1:])
	default:
		return usageErr(fmt.Errorf("unknown keys command %q", args[0]))
	}
}

type keysImportResult struct {
	Imported int `json:"imported"`
	Total    int `json:"total"`
}

func (r *runtime) keysImport(args []string) error {
	fs := flag.NewFlagSet("matcrawl keys import", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	passphraseFlag := fs.String("passphrase", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if fs.NArg() != 1 {
		return usageErr(errors.New("keys import takes exactly one path argument"))
	}
	path := fs.Arg(0)

	passphrase := strings.TrimSpace(*passphraseFlag)
	if passphrase == "" {
		v, err := promptSecret(r.stderr, "key export passphrase: ")
		if err != nil {
			return err
		}
		passphrase = v
	}
	if passphrase == "" {
		// A non-interactive stdin (a pipe, or a shell that doesn't forward
		// the terminal) reads as empty. Decrypting with "" fails as a hash
		// mismatch, which looks like a wrong passphrase, so say so up front.
		return errors.New("no passphrase entered — run `matcrawl keys import` in an interactive terminal, or pipe the passphrase on stdin")
	}

	machine, closeFn, err := r.openCrypto()
	if err != nil {
		return err
	}
	defer closeFn()

	imported, total, err := matrix.ImportKeyExport(r.ctx, machine, path, passphrase)
	if err != nil {
		return fmt.Errorf("import keys: %w", err)
	}
	return r.print(keysImportResult{Imported: imported, Total: total})
}

type keysRetryResult struct {
	Attempted    int `json:"attempted"`
	Decrypted    int `json:"decrypted"`
	StillMissing int `json:"still_missing"`
	Failed       int `json:"failed"`
}

// retryPageSize is how many stored ciphertext events keys retry decrypts and
// writes per transaction.
const retryPageSize = 5000

func (r *runtime) keysRetry(args []string) error {
	fs := flag.NewFlagSet("matcrawl keys retry", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", 0, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	machine, closeFn, err := r.openCrypto()
	if err != nil {
		return err
	}
	defer closeFn()

	return r.withStore(func(st *store.Store) error {
		var result keysRetryResult
		var cursor int64
		for *limit <= 0 || result.Attempted < *limit {
			pageSize := retryPageSize
			if *limit > 0 {
				pageSize = min(pageSize, *limit-result.Attempted)
			}
			pending, next, err := st.ListUndecrypted(r.ctx, cursor, pageSize)
			if err != nil {
				return err
			}
			if len(pending) == 0 {
				break
			}
			cursor = next
			result.Attempted += len(pending)
			if err := retryPage(r, st, machine, pending, &result); err != nil {
				return err
			}
			fmt.Fprintf(r.stderr, "keys retry: %d attempted, %d decrypted, %d still missing, %d failed\n",
				result.Attempted, result.Decrypted, result.StillMissing, result.Failed)
		}
		return r.print(result)
	})
}

// retryPage decrypts one page of stored ciphertext and writes the outcome:
// decrypted events are reclassified, and rows whose status changed (say from
// failed to missing_keys) are updated so status reports stay accurate.
func retryPage(r *runtime, st *store.Store, machine *crypto.OlmMachine, pending []store.Message, result *keysRetryResult) error {
	var decrypted store.Batch
	for _, m := range pending {
		res := matrix.DecryptStored(r.ctx, machine, id.RoomID(m.RoomID), m.RawJSON)
		switch res.Status {
		case "ok":
			b := matrix.DecryptedBatch(m, res.Event)
			decrypted.Messages = append(decrypted.Messages, b.Messages...)
			decrypted.Edits = append(decrypted.Edits, b.Edits...)
			decrypted.Reactions = append(decrypted.Reactions, b.Reactions...)
			decrypted.Redactions = append(decrypted.Redactions, b.Redactions...)
			decrypted.DropMessages = append(decrypted.DropMessages, b.DropMessages...)
			result.Decrypted++
			continue
		case "missing_keys":
			result.StillMissing++
		default:
			res.Status = "failed"
			result.Failed++
		}
		if m.DecryptStatus == res.Status && m.DecryptError == res.ErrorString {
			continue
		}
		m.DecryptStatus = res.Status
		m.DecryptError = res.ErrorString
		decrypted.Messages = append(decrypted.Messages, m)
	}
	if len(decrypted.Messages)+len(decrypted.Edits)+len(decrypted.Reactions)+len(decrypted.Redactions)+len(decrypted.DropMessages) == 0 {
		return nil
	}
	return st.Apply(r.ctx, store.SyncStats{KeepSyncTime: true}, decrypted)
}

func (r *runtime) keysBackupPull(args []string) error {
	fs := flag.NewFlagSet("matcrawl keys backup-pull", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	machine, closeFn, err := r.openCrypto()
	if err != nil {
		return err
	}
	defer closeFn()

	secret, err := promptSecret(r.stderr, "recovery key or security phrase: ")
	if err != nil {
		return err
	}
	if secret == "" {
		return errors.New("no recovery key entered — run `matcrawl keys backup-pull` in an interactive terminal, or pipe the key on stdin")
	}
	backupKey, err := matrix.UnlockKeyBackup(r.ctx, machine, secret)
	if err != nil {
		return err
	}
	result, err := matrix.PullKeyBackup(r.ctx, machine, backupKey, func(p matrix.BackupPullResult) {
		fmt.Fprintf(r.stderr, "keys backup-pull: %d sessions, %d new\n", p.Total, p.New)
	})
	if err != nil {
		return err
	}
	if result.Failed > 0 {
		fmt.Fprintf(r.stderr, "keys backup-pull: %d sessions could not be imported, first: %s\n", result.Failed, result.FirstError)
	}
	if result.New+result.Improved > 0 {
		fmt.Fprintln(r.stderr, "run `matcrawl keys retry` to decrypt stored messages with the new keys")
	}
	return r.print(result)
}

// openCrypto loads the local Olm machine for the configured session without
// touching the server's device keys (see matrix.OpenMachine). The returned
// close function releases the crypto database.
func (r *runtime) openCrypto() (*crypto.OlmMachine, func(), error) {
	session, err := matrix.LoadSession(sessionPath(r.dbPath))
	if err != nil {
		return nil, nil, err
	}
	if session == nil {
		return nil, nil, errors.New("no session — run `matcrawl login` first")
	}
	client, err := matrix.New(*session)
	if err != nil {
		return nil, nil, err
	}
	return matrix.OpenMachine(r.ctx, client, matrix.DefaultCryptoPaths(r.dbPath))
}
