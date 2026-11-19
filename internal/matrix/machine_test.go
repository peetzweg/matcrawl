package matrix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// A stored m.room.encrypted event whose Megolm session was never imported must
// come back as missing_keys, and opening the machine must not touch the
// server: matcrawl shares Element's device ID and must never publish keys.
func TestOpenMachineDecryptsStoredWithoutServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected homeserver request: %s %s", r.Method, r.URL.Path)
		http.Error(w, "no", http.StatusTeapot)
	}))
	defer srv.Close()

	client, err := New(Session{Homeserver: srv.URL, UserID: "@me:example.org", DeviceID: "ELEMENTDEV", AccessToken: "tok"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dir := t.TempDir()
	paths := CryptoPaths{CryptoDB: filepath.Join(dir, "crypto.db"), PickleKey: filepath.Join(dir, "pickle.key")}

	ctx := context.Background()
	machine, closeFn, err := OpenMachine(ctx, client, paths)
	if err != nil {
		t.Fatalf("OpenMachine: %v", err)
	}
	defer closeFn()

	raw := `{"sender":"@alice:example.org","type":"m.room.encrypted","origin_server_ts":1639407567395,` +
		`"event_id":"$evt","room_id":"!room:example.org","content":{"algorithm":"m.megolm.v1.aes-sha2",` +
		`"ciphertext":"` + strings.Repeat("A", 344) + `","device_id":"ALICEDEV","sender_key":"` + strings.Repeat("S", 43) + `",` +
		`"session_id":"` + strings.Repeat("I", 43) + `"}}`
	res := DecryptStored(ctx, machine, "!room:example.org", raw)
	if res.Status != "missing_keys" {
		t.Fatalf("Status = %q (%s), want missing_keys", res.Status, res.ErrorString)
	}
}
