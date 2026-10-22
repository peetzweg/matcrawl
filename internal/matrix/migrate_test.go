package matrix

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/peetzweg/matcrawl/internal/store"
)

func TestMigrateStoredEventsMovesLegacyEdits(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "matcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ts := time.UnixMilli(1000).UTC()
	// What older matcrawl versions stored: the original, its edit as a
	// standalone "* " message, and a server-redacted event.
	legacy := []store.Message{
		{RoomID: string(testRoom), EventID: "$e1", Sender: "@a:x", OriginServerTS: ts, MsgType: "m.text", Body: "teh",
			RawJSON: `{"event_id":"$e1","sender":"@a:x","origin_server_ts":1000,"type":"m.room.message","content":{"msgtype":"m.text","body":"teh"}}`},
		{RoomID: string(testRoom), EventID: "$e2", Sender: "@a:x", OriginServerTS: ts.Add(time.Second), MsgType: "m.text", Body: "* the",
			RawJSON: `{"event_id":"$e2","sender":"@a:x","origin_server_ts":2000,"type":"m.room.message","content":{"msgtype":"m.text","body":"* the","m.new_content":{"msgtype":"m.text","body":"the"},"m.relates_to":{"rel_type":"m.replace","event_id":"$e1"}}}`},
		{RoomID: string(testRoom), EventID: "$e3", Sender: "@a:x", OriginServerTS: ts.Add(2 * time.Second), MsgType: "m.text",
			RawJSON: `{"event_id":"$e3","sender":"@a:x","origin_server_ts":3000,"type":"m.room.message","content":{},"unsigned":{"redacted_because":{"event_id":"$x","sender":"@m:x","origin_server_ts":4000,"type":"m.room.redaction","content":{}}}}`},
	}
	if err := st.Upsert(ctx, store.SyncStats{}, nil, nil, legacy); err != nil {
		t.Fatal(err)
	}
	if err := MigrateStoredEvents(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	msgs, err := st.Messages(ctx, store.MessageFilter{RoomID: string(testRoom), Asc: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || msgs[0].Body != "the" || msgs[0].OriginalBody != "teh" {
		t.Fatalf("after migration: %+v", msgs)
	}
	if msgs[1].Redacted == nil || msgs[1].Redacted.By != "@m:x" {
		t.Fatalf("redaction not recorded: %+v", msgs[1])
	}
	// Idempotent: a second run is a no-op.
	if err := MigrateStoredEvents(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	status, _ := st.Status(ctx)
	if status.Messages != 2 || status.Edits != 1 || status.Redactions != 1 {
		t.Fatalf("status = %+v", status)
	}
}

func TestDecryptedBatch(t *testing.T) {
	placeholder := store.Message{RoomID: string(testRoom), EventID: "$enc", RawJSON: `{"ciphertext":"..."}`}

	msg := DecryptedBatch(placeholder, parseEvent(t, `{"event_id":"$enc","sender":"@a:x","origin_server_ts":1000,"type":"m.room.message","content":{"msgtype":"m.text","body":"secret"}}`))
	if len(msg.Messages) != 1 || msg.Messages[0].Body != "secret" || msg.Messages[0].DecryptStatus != "ok" ||
		!msg.Messages[0].WasEncrypted || msg.Messages[0].RawJSON != placeholder.RawJSON || len(msg.DropMessages) != 0 {
		t.Fatalf("decrypted message batch = %+v", msg)
	}

	react := DecryptedBatch(placeholder, parseEvent(t, `{"event_id":"$enc","sender":"@a:x","origin_server_ts":1000,"type":"m.reaction","content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$m","key":"👍"}}}`))
	if len(react.Reactions) != 1 || !react.Reactions[0].WasEncrypted || len(react.DropMessages) != 1 {
		t.Fatalf("decrypted reaction batch = %+v", react)
	}
}
