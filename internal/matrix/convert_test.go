package matrix

import (
	"encoding/json"
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestEventToMessagePlainText(t *testing.T) {
	raw := []byte(`{
		"event_id": "$evt1",
		"sender": "@alice:example.org",
		"origin_server_ts": 1700000000000,
		"type": "m.room.message",
		"content": {"msgtype": "m.text", "body": "hello world"}
	}`)
	var evt event.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	evt.Type = event.EventMessage
	msg, ok := eventToMessage(id.RoomID("!r:example.org"), &evt)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if msg.EventID != "$evt1" || msg.Sender != "@alice:example.org" {
		t.Errorf("id/sender wrong: %+v", msg)
	}
	if msg.MsgType != "m.text" || msg.Body != "hello world" {
		t.Errorf("body/msgtype wrong: %+v", msg)
	}
	if msg.WasEncrypted {
		t.Error("plain message marked encrypted")
	}
}

func TestEventToMessageEncryptedPlaceholder(t *testing.T) {
	raw := []byte(`{
		"event_id": "$enc1",
		"sender": "@bob:example.org",
		"origin_server_ts": 1700000060000,
		"type": "m.room.encrypted",
		"content": {"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "AAAA"}
	}`)
	var evt event.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	evt.Type = event.EventEncrypted
	msg, ok := eventToMessage(id.RoomID("!r:example.org"), &evt)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !msg.WasEncrypted || msg.DecryptStatus != "missing_keys" {
		t.Errorf("encrypted flags wrong: %+v", msg)
	}
	if msg.Body != "" {
		t.Errorf("ciphertext body leaked: %q", msg.Body)
	}
	if msg.MsgType != "m.room.encrypted" {
		t.Errorf("msgtype = %q, want m.room.encrypted", msg.MsgType)
	}
}

func TestEventToMessageSkipsStateEvents(t *testing.T) {
	raw := []byte(`{
		"event_id": "$state1",
		"sender": "@a:x",
		"origin_server_ts": 1700000000000,
		"type": "m.room.member",
		"state_key": "@a:x",
		"content": {"membership": "join"}
	}`)
	var evt event.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	evt.Type = event.StateMember
	if _, ok := eventToMessage(id.RoomID("!r:x"), &evt); ok {
		t.Error("member state event should not produce a message row")
	}
}

func TestStateAccumulatorRoomName(t *testing.T) {
	a := newStateAccumulator(id.RoomID("!r:x"))
	raw := []byte(`{"event_id":"$n","sender":"@a:x","origin_server_ts":1700000000000,"type":"m.room.name","state_key":"","content":{"name":"my room"}}`)
	var evt event.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	evt.Type = event.StateRoomName
	a.apply(&evt)
	if got := a.result().Name; got != "my room" {
		t.Errorf("Name = %q, want %q", got, "my room")
	}
}

func TestStateAccumulatorEncryption(t *testing.T) {
	a := newStateAccumulator(id.RoomID("!r:x"))
	raw := []byte(`{"event_id":"$e","sender":"@a:x","origin_server_ts":1700000000000,"type":"m.room.encryption","state_key":"","content":{"algorithm":"m.megolm.v1.aes-sha2"}}`)
	var evt event.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	evt.Type = event.StateEncryption
	a.apply(&evt)
	r := a.result()
	if !r.IsEncrypted || r.EncryptionAlgorithm != "m.megolm.v1.aes-sha2" {
		t.Errorf("encryption state not applied: %+v", r)
	}
}

func TestMemberAccumulator(t *testing.T) {
	ma := newMemberAccumulator(id.RoomID("!r:x"))
	raw1 := []byte(`{"event_id":"$m1","sender":"@a:x","origin_server_ts":1700000000000,"type":"m.room.member","state_key":"@a:x","content":{"membership":"join","displayname":"Alice"}}`)
	raw2 := []byte(`{"event_id":"$m2","sender":"@b:x","origin_server_ts":1700000010000,"type":"m.room.member","state_key":"@b:x","content":{"membership":"invite"}}`)
	for _, raw := range [][]byte{raw1, raw2} {
		var evt event.Event
		if err := json.Unmarshal(raw, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		evt.Type = event.StateMember
		ma.apply(&evt)
	}
	got := ma.result()
	if len(got) != 2 {
		t.Fatalf("members = %d, want 2", len(got))
	}
}
