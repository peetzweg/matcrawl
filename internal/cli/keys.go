package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"maunium.net/go/mautrix/crypto/cryptohelper"

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

	helper, closeFn, err := r.openCrypto()
	if err != nil {
		return err
	}
	defer closeFn()

	imported, total, err := matrix.ImportKeyExport(r.ctx, helper, path, passphrase)
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

func (r *runtime) keysRetry(args []string) error {
	fs := flag.NewFlagSet("matcrawl keys retry", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", 5000, "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	helper, closeFn, err := r.openCrypto()
	if err != nil {
		return err
	}
	defer closeFn()

	return r.withStore(func(st *store.Store) error {
		pending, err := st.ListUndecrypted(r.ctx, *limit)
		if err != nil {
			return err
		}
		result := keysRetryResult{Attempted: len(pending)}
		var updates []store.Message
		for _, m := range pending {
			res := matrix.DecryptStored(r.ctx, helper, m.RawJSON)
			switch res.Status {
			case "ok":
				m.Body = res.Body
				m.FormattedBody = res.FormattedBody
				if res.MsgType != "" {
					m.MsgType = res.MsgType
				}
				m.DecryptStatus = "ok"
				m.DecryptError = ""
				result.Decrypted++
				updates = append(updates, m)
			case "missing_keys":
				m.DecryptStatus = "missing_keys"
				m.DecryptError = res.ErrorString
				result.StillMissing++
			default:
				m.DecryptStatus = "failed"
				m.DecryptError = res.ErrorString
				result.Failed++
				updates = append(updates, m)
			}
		}
		if len(updates) > 0 {
			if err := st.Upsert(r.ctx, store.SyncStats{}, nil, nil, updates); err != nil {
				return err
			}
		}
		return r.print(result)
	})
}

func (r *runtime) keysBackupPull(args []string) error {
	fs := flag.NewFlagSet("matcrawl keys backup-pull", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	return errors.New("server-side key backup pull is not implemented yet — export keys from Element (Settings → Security & Privacy → Export E2E room keys) and use `matcrawl keys import <path>` for now")
}

// openCrypto returns an initialised CryptoHelper backed by the configured
// session + db paths. The returned close function tears the helper down.
func (r *runtime) openCrypto() (*cryptohelper.CryptoHelper, func(), error) {
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
	helper, err := matrix.OpenCrypto(r.ctx, client, matrix.DefaultCryptoPaths(r.dbPath))
	if err != nil {
		return nil, nil, err
	}
	closeFn := func() {
		_ = helper.Close()
	}
	return helper, closeFn, nil
}
