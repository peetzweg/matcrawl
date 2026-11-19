package matrix

import (
	"context"
	"errors"
	"fmt"

	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/crypto/backup"
	"maunium.net/go/mautrix/crypto/ssss"
	"maunium.net/go/mautrix/event"
)

// ErrWrongRecoveryKey means the recovery key or security phrase did not
// unlock secret storage.
var ErrWrongRecoveryKey = errors.New("recovery key or security phrase does not unlock secret storage")

// UnlockKeyBackup turns the user's recovery key (the "EsT…" string Element
// shows when setting up recovery) or security phrase into the Megolm backup
// decryption key, read from secret storage in the user's account data.
//
// Only reads account data; nothing is written to the server.
func UnlockKeyBackup(ctx context.Context, machine *crypto.OlmMachine, secret string) (*backup.MegolmBackupKey, error) {
	keyID, keyData, err := machine.SSSS.GetDefaultKeyData(ctx)
	if err != nil {
		return nil, fmt.Errorf("read secret storage key: %w", err)
	}
	key, err := keyData.VerifyRecoveryKey(keyID, secret)
	if errors.Is(err, ssss.ErrInvalidRecoveryKey) && keyData.Passphrase != nil {
		key, err = keyData.VerifyPassphrase(keyID, secret)
	}
	switch {
	case errors.Is(err, ssss.ErrInvalidRecoveryKey):
		return nil, fmt.Errorf("%w: not a valid recovery key", ErrWrongRecoveryKey)
	case errors.Is(err, ssss.ErrUnverifiableKey):
		// Old secret storage keys carry no check value; the MAC on the
		// backup secret below still catches a wrong key.
	case err != nil:
		return nil, fmt.Errorf("%w: %w", ErrWrongRecoveryKey, err)
	}

	raw, err := machine.SSSS.GetDecryptedAccountData(ctx, event.AccountDataMegolmBackupKey, key)
	if errors.Is(err, ssss.ErrKeyDataMACMismatch) {
		return nil, ErrWrongRecoveryKey
	} else if err != nil {
		return nil, fmt.Errorf("read key backup secret from secret storage: %w", err)
	}
	backupKey, err := backup.MegolmBackupKeyFromBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("decode key backup secret: %w", err)
	}
	return backupKey, nil
}

// BackupPullResult summarises a key backup download.
type BackupPullResult struct {
	Version string `json:"version"`
	Total   int    `json:"total"`
	// New sessions were not in the local store before.
	New int `json:"new"`
	// Improved sessions were known but only from a later message on; the
	// backup copy decrypts earlier messages too.
	Improved int `json:"improved"`
	Known    int `json:"already_known"`
	Failed   int `json:"failed"`
}

// PullKeyBackup downloads the latest server-side key backup and stores every
// Megolm session it holds. The backup is trusted because its public key
// matches the private key from secret storage. Existing sessions are only
// replaced by copies that start at an earlier message index.
//
// Only reads from the server. progress, if set, is called periodically.
func PullKeyBackup(ctx context.Context, machine *crypto.OlmMachine, backupKey *backup.MegolmBackupKey, progress func(BackupPullResult)) (BackupPullResult, error) {
	var res BackupPullResult
	info, err := machine.GetAndVerifyLatestKeyBackupVersion(ctx, backupKey)
	if err != nil {
		return res, fmt.Errorf("check key backup: %w", err)
	} else if info == nil {
		return res, errors.New("this account has no server-side key backup")
	}
	res.Version = string(info.Version)

	keys, err := machine.Client.GetKeyBackup(ctx, info.Version)
	if err != nil {
		return res, fmt.Errorf("download key backup: %w", err)
	}
	for roomID, room := range keys.Rooms {
		for sessionID, data := range room.Sessions {
			res.Total++
			existing, err := machine.CryptoStore.GetGroupSession(ctx, roomID, sessionID)
			if err != nil && !errors.Is(err, crypto.ErrGroupSessionWithheld) {
				return res, fmt.Errorf("look up session %s: %w", sessionID, err)
			}
			sess, err := data.SessionData.Decrypt(backupKey)
			if err != nil {
				res.Failed++
				continue
			}
			imported, err := machine.ImportRoomKeyFromBackup(ctx, info.Version, roomID, sessionID, sess)
			switch {
			case err != nil:
				res.Failed++
			case existing == nil:
				res.New++
			case imported.Internal.FirstKnownIndex() < existing.Internal.FirstKnownIndex():
				res.Improved++
			default:
				res.Known++
			}
			if progress != nil && res.Total%5000 == 0 {
				progress(res)
			}
		}
	}
	return res, nil
}
