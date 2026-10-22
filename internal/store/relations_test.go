package store

import (
	"testing"
	"time"
)

func TestRelationsResolveRegardlessOfArrivalOrder(t *testing.T) {
	st, ctx := openTestStore(t)
	ts := time.Unix(1_700_000_000, 0).UTC()
	room := "!r:x"
	msg := Message{RoomID: room, EventID: "$m", Sender: "@a:x", OriginServerTS: ts, MsgType: "m.text", Body: "helo wrld"}
	rel := Batch{
		Edits: []Edit{
			{RoomID: room, EventID: "$e1", TargetEventID: "$m", Sender: "@a:x", OriginServerTS: ts.Add(time.Minute), Body: "hello world"},
			{RoomID: room, EventID: "$e2", TargetEventID: "$m", Sender: "@a:x", OriginServerTS: ts.Add(2 * time.Minute), Body: "hello there world"},
			// Only the original sender may edit; this one must be ignored.
			{RoomID: room, EventID: "$e3", TargetEventID: "$m", Sender: "@mallory:x", OriginServerTS: ts.Add(3 * time.Minute), Body: "pwned"},
		},
		Reactions: []Reaction{
			{RoomID: room, EventID: "$r1", TargetEventID: "$m", Sender: "@b:x", Key: "👍", OriginServerTS: ts},
			{RoomID: room, EventID: "$r2", TargetEventID: "$m", Sender: "@c:x", Key: "👍", OriginServerTS: ts},
			{RoomID: room, EventID: "$r3", TargetEventID: "$m", Sender: "@c:x", Key: "🎉", OriginServerTS: ts},
		},
		// The 🎉 reaction was taken back.
		Redactions: []Redaction{{RoomID: room, EventID: "$x1", TargetEventID: "$r3", Sender: "@c:x", OriginServerTS: ts}},
	}
	// Backfill walks backwards: relations arrive before their target.
	if err := st.Apply(ctx, SyncStats{}, rel); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(ctx, SyncStats{}, Batch{Messages: []Message{msg}}); err != nil {
		t.Fatal(err)
	}

	got, err := st.Messages(ctx, MessageFilter{RoomID: room})
	if err != nil || len(got) != 1 {
		t.Fatalf("messages = %+v, %v", got, err)
	}
	m := got[0]
	if m.Body != "hello there world" || m.OriginalBody != "helo wrld" || len(m.Edits) != 2 || m.EditedAt.IsZero() {
		t.Fatalf("edit view = body %q original %q edits %d", m.Body, m.OriginalBody, len(m.Edits))
	}
	if len(m.Reactions) != 1 || m.Reactions[0].Key != "👍" || m.Reactions[0].Count != 2 {
		t.Fatalf("reactions = %+v", m.Reactions)
	}
	if m.Redacted != nil {
		t.Fatalf("message itself was not deleted: %+v", m.Redacted)
	}

	// Search finds the current text, not the typo'd original.
	if hits, _ := st.Search(ctx, MessageFilter{Query: "there"}); len(hits) != 1 {
		t.Fatalf("search for edited text = %d hits", len(hits))
	}
	if hits, _ := st.Search(ctx, MessageFilter{Query: "wrld"}); len(hits) != 0 {
		t.Fatalf("search still finds pre-edit text: %d hits", len(hits))
	}

	// Deleting the latest edit falls back to the previous one, in search too.
	if err := st.Apply(ctx, SyncStats{}, Batch{Redactions: []Redaction{{RoomID: room, EventID: "$x2", TargetEventID: "$e2", Sender: "@a:x", OriginServerTS: ts}}}); err != nil {
		t.Fatal(err)
	}
	got, _ = st.Messages(ctx, MessageFilter{RoomID: room})
	if got[0].Body != "hello world" {
		t.Fatalf("after deleting an edit, body = %q", got[0].Body)
	}
	if hits, _ := st.Search(ctx, MessageFilter{Query: "there"}); len(hits) != 0 {
		t.Fatalf("search still finds the deleted edit")
	}

	// Deleting the message marks it but keeps what was archived.
	if err := st.Apply(ctx, SyncStats{}, Batch{Redactions: []Redaction{{RoomID: room, EventID: "$x3", TargetEventID: "$m", Sender: "@mod:x", OriginServerTS: ts, Reason: "spam"}}}); err != nil {
		t.Fatal(err)
	}
	got, _ = st.Messages(ctx, MessageFilter{RoomID: room})
	if got[0].Redacted == nil || got[0].Redacted.By != "@mod:x" || got[0].Redacted.Reason != "spam" || got[0].Body == "" {
		t.Fatalf("redacted view = %+v body %q", got[0].Redacted, got[0].Body)
	}

	st2, _ := st.Status(ctx)
	if st2.Edits != 3 || st2.Reactions != 3 || st2.Redactions != 3 || st2.Messages != 1 {
		t.Fatalf("status = %+v", st2)
	}
}

func TestDropMessagesRemovesPlaceholder(t *testing.T) {
	st, ctx := openTestStore(t)
	ts := time.Unix(1_700_000_000, 0).UTC()
	enc := Message{RoomID: "!r:x", EventID: "$enc", Sender: "@a:x", OriginServerTS: ts, MsgType: "m.room.encrypted", WasEncrypted: true, DecryptStatus: "missing_keys"}
	if err := st.Upsert(ctx, SyncStats{}, []Room{{ID: "!r:x"}}, nil, []Message{enc}); err != nil {
		t.Fatal(err)
	}
	// It decrypted to a reaction.
	b := Batch{
		Reactions:    []Reaction{{RoomID: "!r:x", EventID: "$enc", TargetEventID: "$m", Sender: "@a:x", Key: "👍", OriginServerTS: ts, WasEncrypted: true}},
		DropMessages: []EventRef{{RoomID: "!r:x", EventID: "$enc"}},
	}
	if err := st.Apply(ctx, SyncStats{}, b); err != nil {
		t.Fatal(err)
	}
	status, _ := st.Status(ctx)
	if status.Messages != 0 || status.Reactions != 1 {
		t.Fatalf("status = %+v", status)
	}
	rooms, _ := st.ListRooms(ctx, RoomFilter{})
	if rooms[0].MessageCount != 0 {
		t.Fatalf("message_count not refreshed: %d", rooms[0].MessageCount)
	}
}

func TestPowerLevelsAndSpaces(t *testing.T) {
	st, ctx := openTestStore(t)
	b := Batch{
		Rooms: []Room{
			{ID: "!space:x", Name: "Parity", RoomType: "m.space"},
			{ID: "!r:x", Name: "Ops", PowerLevelsJSON: `{"users":{"@admin:x":100,"@mod:x":50},"users_default":10}`},
		},
		Members: []RoomMember{
			{RoomID: "!r:x", UserID: "@admin:x", Membership: "join"},
			{RoomID: "!r:x", UserID: "@mod:x", Membership: "join"},
			{RoomID: "!r:x", UserID: "@pleb:x", Membership: "join"},
		},
		SpaceChildren: []SpaceChild{
			{SpaceID: "!space:x", ChildID: "!r:x", Via: []string{"x"}},
			{SpaceID: "!space:x", ChildID: "!notjoined:x", Via: []string{"x"}},
		},
	}
	if err := st.Apply(ctx, SyncStats{}, b); err != nil {
		t.Fatal(err)
	}
	members, err := st.ListMembers(ctx, "!r:x")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"@admin:x": 100, "@mod:x": 50, "@pleb:x": 10}
	for _, m := range members {
		if m.PowerLevel != want[m.UserID] {
			t.Errorf("%s power = %d, want %d", m.UserID, m.PowerLevel, want[m.UserID])
		}
	}
	if members[0].UserID != "@admin:x" {
		t.Errorf("members not ordered by power: %+v", members)
	}

	inSpace, _ := st.ListRooms(ctx, RoomFilter{SpaceID: "!space:x"})
	if len(inSpace) != 1 || inSpace[0].ID != "!r:x" || len(inSpace[0].ParentSpaces) != 1 || inSpace[0].ParentSpaces[0].Name != "Parity" {
		t.Fatalf("rooms in space = %+v", inSpace)
	}
	tree, _ := st.SpaceChildren(ctx)
	if len(tree["!space:x"]) != 2 {
		t.Fatalf("space children = %+v", tree)
	}

	// Removing a child.
	if err := st.Apply(ctx, SyncStats{}, Batch{SpaceChildren: []SpaceChild{{SpaceID: "!space:x", ChildID: "!r:x", Removed: true}}}); err != nil {
		t.Fatal(err)
	}
	if inSpace, _ := st.ListRooms(ctx, RoomFilter{SpaceID: "!space:x"}); len(inSpace) != 0 {
		t.Fatalf("removed child still listed: %+v", inSpace)
	}
}

func TestSnapshotRoundTripKeepsRelations(t *testing.T) {
	src, ctx := openTestStore(t)
	ts := time.Unix(1_700_000_000, 0).UTC()
	b := Batch{
		Rooms:         []Room{{ID: "!r:x", Name: "Ops", RoomType: "m.space", PowerLevelsJSON: `{"users_default":5}`}},
		Messages:      []Message{{RoomID: "!r:x", EventID: "$m", Sender: "@a:x", OriginServerTS: ts, MsgType: "m.text", Body: "v1"}},
		Edits:         []Edit{{RoomID: "!r:x", EventID: "$e", TargetEventID: "$m", Sender: "@a:x", OriginServerTS: ts.Add(time.Second), Body: "v2"}},
		Reactions:     []Reaction{{RoomID: "!r:x", EventID: "$r", TargetEventID: "$m", Sender: "@b:x", Key: "👍", OriginServerTS: ts}},
		Redactions:    []Redaction{{RoomID: "!r:x", EventID: "$x", TargetEventID: "$gone", Sender: "@b:x", OriginServerTS: ts}},
		SpaceChildren: []SpaceChild{{SpaceID: "!r:x", ChildID: "!c:x", Via: []string{"x"}, Suggested: true}},
	}
	if err := src.Apply(ctx, SyncStats{}, b); err != nil {
		t.Fatal(err)
	}
	snap, err := src.ExportAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dst, _ := openTestStore(t)
	if err := dst.ImportSnapshot(ctx, snap, "test", ts); err != nil {
		t.Fatal(err)
	}
	got, _ := dst.Messages(ctx, MessageFilter{RoomID: "!r:x"})
	if len(got) != 1 || got[0].Body != "v2" || len(got[0].Reactions) != 1 {
		t.Fatalf("restored message = %+v", got)
	}
	status, _ := dst.Status(ctx)
	if status.Edits != 1 || status.Reactions != 1 || status.Redactions != 1 || status.Spaces != 1 {
		t.Fatalf("restored status = %+v", status)
	}
	tree, _ := dst.SpaceChildren(ctx)
	if len(tree["!r:x"]) != 1 {
		t.Fatalf("restored space children = %+v", tree)
	}
}

func TestHasAnyEvent(t *testing.T) {
	st, ctx := openTestStore(t)
	ts := time.Unix(1_700_000_000, 0).UTC()
	if err := st.Apply(ctx, SyncStats{}, Batch{Reactions: []Reaction{{RoomID: "!r:x", EventID: "$r", TargetEventID: "$m", Sender: "@a:x", Key: "x", OriginServerTS: ts}}}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.HasAnyEvent(ctx, "!r:x", []string{"$nope", "$r"}); !ok {
		t.Fatal("known reaction not found")
	}
	if ok, _ := st.HasAnyEvent(ctx, "!r:x", []string{"$nope"}); ok {
		t.Fatal("unknown event reported as known")
	}
	if ok, _ := st.HasAnyEvent(ctx, "!other:x", []string{"$r"}); ok {
		t.Fatal("event matched in the wrong room")
	}
}
