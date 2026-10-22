package matrix

import (
	"encoding/json"
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/peetzweg/matcrawl/internal/store"
)

const testRoom = id.RoomID("!r:example.org")

func parseEvent(t *testing.T, raw string) *event.Event {
	t.Helper()
	var evt event.Event
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &evt
}

func classify(t *testing.T, raw string) store.Batch {
	t.Helper()
	var b store.Batch
	appendEvent(&b, testRoom, parseEvent(t, raw))
	return b
}

func TestAppendEventEdit(t *testing.T) {
	b := classify(t, `{"event_id":"$e2","sender":"@a:x","origin_server_ts":2000,"type":"m.room.message",
		"content":{"msgtype":"m.text","body":"* fixed","m.new_content":{"msgtype":"m.text","body":"fixed"},
		"m.relates_to":{"rel_type":"m.replace","event_id":"$e1"}}}`)
	if len(b.Messages) != 0 || len(b.Edits) != 1 {
		t.Fatalf("want one edit and no message, got %+v", b)
	}
	if e := b.Edits[0]; e.TargetEventID != "$e1" || e.Body != "fixed" || e.Sender != "@a:x" {
		t.Fatalf("edit = %+v", e)
	}
}

func TestAppendEventEditWithoutNewContentStripsFallback(t *testing.T) {
	b := classify(t, `{"event_id":"$e2","sender":"@a:x","origin_server_ts":2000,"type":"m.room.message",
		"content":{"msgtype":"m.text","body":"* fixed","m.relates_to":{"rel_type":"m.replace","event_id":"$e1"}}}`)
	if len(b.Edits) != 1 || b.Edits[0].Body != "fixed" {
		t.Fatalf("edits = %+v", b.Edits)
	}
}

func TestAppendEventReaction(t *testing.T) {
	b := classify(t, `{"event_id":"$r1","sender":"@b:x","origin_server_ts":3000,"type":"m.reaction",
		"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$e1","key":"👍"}}}`)
	if len(b.Reactions) != 1 || b.Reactions[0].Key != "👍" || b.Reactions[0].TargetEventID != "$e1" {
		t.Fatalf("reactions = %+v", b.Reactions)
	}
}

func TestAppendEventRedaction(t *testing.T) {
	// v11 rooms carry the target in content, older rooms at the top level.
	for _, raw := range []string{
		`{"event_id":"$x1","sender":"@m:x","origin_server_ts":4000,"type":"m.room.redaction","content":{"redacts":"$e1","reason":"spam"}}`,
		`{"event_id":"$x1","sender":"@m:x","origin_server_ts":4000,"type":"m.room.redaction","redacts":"$e1","content":{"reason":"spam"}}`,
	} {
		b := classify(t, raw)
		if len(b.Redactions) != 1 || b.Redactions[0].TargetEventID != "$e1" || b.Redactions[0].Reason != "spam" {
			t.Fatalf("redactions = %+v (%s)", b.Redactions, raw)
		}
		if len(b.Messages) != 0 {
			t.Fatalf("redaction archived as message: %+v", b.Messages)
		}
	}
}

func TestAppendEventAlreadyRedacted(t *testing.T) {
	b := classify(t, `{"event_id":"$e1","sender":"@a:x","origin_server_ts":1000,"type":"m.room.message","content":{},
		"unsigned":{"redacted_because":{"event_id":"$x1","sender":"@m:x","origin_server_ts":4000,"type":"m.room.redaction","content":{"reason":"oops"}}}}`)
	if len(b.Messages) != 1 || b.Messages[0].Body != "" {
		t.Fatalf("redacted message should still be archived empty: %+v", b.Messages)
	}
	if len(b.Redactions) != 1 || b.Redactions[0].TargetEventID != "$e1" || b.Redactions[0].Sender != "@m:x" {
		t.Fatalf("redactions = %+v", b.Redactions)
	}
}

func TestAppendEventStickerAndPoll(t *testing.T) {
	b := classify(t, `{"event_id":"$s1","sender":"@a:x","origin_server_ts":1000,"type":"m.sticker","content":{"body":"party","url":"mxc://x/y","info":{}}}`)
	if len(b.Messages) != 1 || b.Messages[0].MsgType != "m.sticker" || b.Messages[0].Body != "party" {
		t.Fatalf("sticker = %+v", b.Messages)
	}
	b = classify(t, `{"event_id":"$p1","sender":"@a:x","origin_server_ts":1000,"type":"org.matrix.msc3381.poll.start",
		"content":{"org.matrix.msc3381.poll.start":{"question":{"org.matrix.msc1767.text":"Lunch?"},"answers":[]}}}`)
	if len(b.Messages) != 1 || b.Messages[0].Body != "Lunch?" || b.Messages[0].MsgType != "org.matrix.msc3381.poll.start" {
		t.Fatalf("poll = %+v", b.Messages)
	}
	b = classify(t, `{"event_id":"$v1","sender":"@a:x","origin_server_ts":1000,"type":"org.matrix.msc3381.poll.response","content":{}}`)
	if len(b.Messages) != 0 {
		t.Fatalf("poll vote should not be archived as a message: %+v", b.Messages)
	}
}

func TestStateAccumulatorRoomMetadata(t *testing.T) {
	sa := newStateAccumulator(testRoom)
	for _, raw := range []string{
		`{"event_id":"$c","sender":"@creator:x","origin_server_ts":1000,"type":"m.room.create","state_key":"",
			"content":{"room_version":"10","type":"m.space","predecessor":{"room_id":"!old:x","event_id":"$t"}}}`,
		`{"event_id":"$t","sender":"@creator:x","origin_server_ts":2000,"type":"m.room.tombstone","state_key":"",
			"content":{"body":"moved","replacement_room":"!new:x"}}`,
		`{"event_id":"$p","sender":"@creator:x","origin_server_ts":3000,"type":"m.room.power_levels","state_key":"",
			"content":{"users":{"@creator:x":100},"users_default":0}}`,
		`{"event_id":"$k1","sender":"@creator:x","origin_server_ts":4000,"type":"m.space.child","state_key":"!child:x",
			"content":{"via":["x"],"suggested":true}}`,
		`{"event_id":"$k2","sender":"@creator:x","origin_server_ts":5000,"type":"m.space.child","state_key":"!gone:x","content":{}}`,
	} {
		sa.apply(parseEvent(t, raw))
	}
	r := sa.result()
	if r.RoomType != "m.space" || r.RoomVersion != "10" || r.Creator != "@creator:x" || r.CreatedAt.IsZero() ||
		r.PredecessorRoomID != "!old:x" || r.ReplacementRoomID != "!new:x" || r.PowerLevelsJSON == "" {
		t.Fatalf("room = %+v", r)
	}
	kids := map[string]store.SpaceChild{}
	for _, c := range sa.spaceChildren() {
		kids[c.ChildID] = c
	}
	if c := kids["!child:x"]; c.Removed || !c.Suggested || len(c.Via) != 1 {
		t.Fatalf("child = %+v", c)
	}
	if !kids["!gone:x"].Removed {
		t.Fatalf("empty m.space.child should mark removal: %+v", kids["!gone:x"])
	}
}

func TestAppendEventPlainMessageKeepsBody(t *testing.T) {
	// Regression: classifying parses content more than once; the second
	// parse must not lose the body.
	b := classify(t, `{"event_id":"$m","sender":"@a:x","origin_server_ts":1000,"type":"m.room.message",
		"content":{"msgtype":"m.text","body":"hello","m.relates_to":{"m.in_reply_to":{"event_id":"$p"}}}}`)
	if len(b.Messages) != 1 || b.Messages[0].Body != "hello" || b.Messages[0].ReplyToEventID != "$p" {
		t.Fatalf("messages = %+v", b.Messages)
	}
}
