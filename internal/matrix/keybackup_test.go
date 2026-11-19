package matrix

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/backup"
	"maunium.net/go/mautrix/crypto/olm"
	"maunium.net/go/mautrix/crypto/ssss"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// End to end against a fake homeserver: unlock secret storage with the
// recovery key, pull the backup, and decrypt a real Megolm message with the
// session it held. The server rejects every write.
func TestPullKeyBackupDecryptsStoredMessage(t *testing.T) {
	const roomID = id.RoomID("!room:example.org")
	senderKey := strings.Repeat("S", 43)

	ssssKey, err := ssss.NewKey("")
	if err != nil {
		t.Fatal(err)
	}
	backupKey, err := backup.NewMegolmBackupKey()
	if err != nil {
		t.Fatal(err)
	}

	outbound, err := olm.NewOutboundGroupSession()
	if err != nil {
		t.Fatal(err)
	}
	inbound, err := olm.NewInboundGroupSession([]byte(outbound.Key()))
	if err != nil {
		t.Fatal(err)
	}
	exported, err := inbound.Export(0)
	if err != nil {
		t.Fatal(err)
	}
	sessionData, err := backup.EncryptSessionData(backupKey, backup.MegolmSessionData{
		Algorithm:         id.AlgorithmMegolmV1,
		SenderKey:         id.SenderKey(senderKey),
		SenderClaimedKeys: backup.SenderClaimedKeys{Ed25519: id.Ed25519(strings.Repeat("E", 43))},
		SessionKey:        string(exported),
	})
	if err != nil {
		t.Fatal(err)
	}
	plaintext, _ := json.Marshal(map[string]any{
		"room_id": roomID, "type": "m.room.message",
		"content": map[string]any{"msgtype": "m.text", "body": "hello from the backup"},
	})
	ciphertext, err := outbound.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"sender": "@alice:example.org", "type": "m.room.encrypted", "origin_server_ts": 1_700_000_000_000,
		"event_id": "$evt", // no room_id: stored the way /sync delivers it
		"content": map[string]any{
			"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": string(ciphertext),
			"device_id": "ALICEDEV", "sender_key": senderKey, "session_id": outbound.ID(),
		},
	})

	routes := map[string]any{
		"/account_data/m.secret_storage.default_key":       map[string]string{"key": ssssKey.ID},
		"/account_data/m.secret_storage.key." + ssssKey.ID: ssssKey.Metadata,
		"/account_data/" + event.AccountDataMegolmBackupKey.Type: ssss.EncryptedAccountDataEventContent{
			Encrypted: map[string]ssss.EncryptedKeyData{ssssKey.ID: ssssKey.Encrypt(event.AccountDataMegolmBackupKey.Type, backupKey.Bytes())},
		},
		"/room_keys/version": mautrix.RespRoomKeysVersion[backup.MegolmAuthData]{
			Algorithm: id.KeyBackupAlgorithmMegolmBackupV1, Count: 1, ETag: "1", Version: "1",
			AuthData: backup.MegolmAuthData{PublicKey: id.Ed25519(base64.RawStdEncoding.EncodeToString(backupKey.PublicKey().Bytes()))},
		},
		// Marshal through the pointer: EphemeralKey only has a pointer
		// MarshalJSON, and a real server sends it as a string.
		"/room_keys/keys": mautrix.RespRoomKeys[json.RawMessage]{
			Rooms: map[id.RoomID]mautrix.RespRoomKeyBackup[json.RawMessage]{
				roomID: {Sessions: map[id.SessionID]mautrix.RespKeyBackupData[json.RawMessage]{
					outbound.ID(): {SessionData: mustJSON(t, sessionData)},
				}},
			},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected write: %s %s", r.Method, r.URL.Path)
			http.Error(w, "read-only", http.StatusForbidden)
			return
		}
		for suffix, body := range routes {
			if strings.HasSuffix(r.URL.Path, suffix) {
				_ = json.NewEncoder(w).Encode(body)
				return
			}
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errcode":"M_NOT_FOUND"}`))
	}))
	defer srv.Close()

	client, err := New(Session{Homeserver: srv.URL, UserID: "@me:example.org", DeviceID: "ELEMENTDEV", AccessToken: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ctx := context.Background()
	machine, closeFn, err := OpenMachine(ctx, client, CryptoPaths{CryptoDB: filepath.Join(dir, "crypto.db"), PickleKey: filepath.Join(dir, "pickle.key")})
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()

	if res := DecryptStored(ctx, machine, roomID, string(raw)); res.Status != "missing_keys" {
		t.Fatalf("before pull: status %q (%s), want missing_keys", res.Status, res.ErrorString)
	}

	otherKey, _ := ssss.NewKey("")
	if _, err := UnlockKeyBackup(ctx, machine, otherKey.RecoveryKey()); !errors.Is(err, ErrWrongRecoveryKey) {
		t.Fatalf("wrong recovery key: err = %v, want ErrWrongRecoveryKey", err)
	}
	if _, err := UnlockKeyBackup(ctx, machine, "not a key"); !errors.Is(err, ErrWrongRecoveryKey) {
		t.Fatalf("garbage recovery key: err = %v, want ErrWrongRecoveryKey", err)
	}

	unlocked, err := UnlockKeyBackup(ctx, machine, ssssKey.RecoveryKey())
	if err != nil {
		t.Fatalf("UnlockKeyBackup: %v", err)
	}
	got, err := PullKeyBackup(ctx, machine, unlocked, nil)
	if err != nil {
		t.Fatalf("PullKeyBackup: %v", err)
	}
	if got.Total != 1 || got.New != 1 || got.Failed != 0 {
		t.Fatalf("first pull = %+v, want 1 new", got)
	}

	res := DecryptStored(ctx, machine, roomID, string(raw))
	if res.Status != "ok" || res.Body != "hello from the backup" {
		t.Fatalf("after pull: status %q body %q (%s)", res.Status, res.Body, res.ErrorString)
	}

	again, err := PullKeyBackup(ctx, machine, unlocked, nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.Known != 1 || again.New != 0 {
		t.Fatalf("second pull = %+v, want 1 already known", again)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	buf, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}
