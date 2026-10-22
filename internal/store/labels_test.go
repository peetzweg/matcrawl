package store

import (
	"testing"
	"time"
)

func TestUpsertKeepsRoomStateMissingFromIncrementalSync(t *testing.T) {
	st, ctx := openTestStore(t)
	full := Room{ID: "!r:example.org", Name: "Ops", CanonicalAlias: "#ops:example.org", Topic: "pager", IsEncrypted: true, EncryptionAlgorithm: "m.megolm.v1.aes-sha2", MemberCount: 7}
	if err := st.Upsert(ctx, SyncStats{}, []Room{full}, nil, nil); err != nil {
		t.Fatal(err)
	}
	// An incremental /sync or backfill page: only the ID and a new token.
	if err := st.Upsert(ctx, SyncStats{}, []Room{{ID: full.ID, PrevBatch: "t2"}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	rooms, err := st.ListRooms(ctx, RoomFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := rooms[0]
	if got.Name != "Ops" || got.CanonicalAlias != full.CanonicalAlias || got.Topic != "pager" ||
		!got.IsEncrypted || got.EncryptionAlgorithm != full.EncryptionAlgorithm || got.MemberCount != 7 {
		t.Fatalf("room state was clobbered: %#v", got)
	}
	if got.DisplayName != "Ops" {
		t.Fatalf("display name = %q", got.DisplayName)
	}

	// A real rename still lands.
	if err := st.Upsert(ctx, SyncStats{}, []Room{{ID: full.ID, Name: "Ops 2"}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	rooms, _ = st.ListRooms(ctx, RoomFilter{Limit: 10})
	if rooms[0].Name != "Ops 2" {
		t.Fatalf("rename lost: %q", rooms[0].Name)
	}
}

func TestRoomLabels(t *testing.T) {
	st, ctx := openTestStore(t)
	if err := st.SetSyncState(ctx, "user_id", "@me:example.org"); err != nil {
		t.Fatal(err)
	}
	rooms := []Room{
		{ID: "!named:x", Name: "Named"},
		{ID: "!alias:x", CanonicalAlias: "#alias:x"},
		{ID: "!dm:x"},
		{ID: "!group:x"},
		{ID: "!big:x"},
		{ID: "!six:x"},
		{ID: "!empty:x"},
		{ID: "!nothing:x"},
	}
	members := []RoomMember{
		{RoomID: "!dm:x", UserID: "@me:example.org", DisplayName: "Me", Membership: "join"},
		{RoomID: "!dm:x", UserID: "@alice:x", DisplayName: "Alice", Membership: "join"},
		{RoomID: "!group:x", UserID: "@alice:x", DisplayName: "Alice", Membership: "join"},
		{RoomID: "!group:x", UserID: "@bob:x", Membership: "invite"},
		{RoomID: "!group:x", UserID: "@carol:x", DisplayName: "Carol", Membership: "leave"},
		{RoomID: "!empty:x", UserID: "@dave:x", DisplayName: "Dave", Membership: "leave"},
	}
	for _, u := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		members = append(members, RoomMember{RoomID: "!big:x", UserID: "@" + u + ":x", DisplayName: u, Membership: "join"})
	}
	for _, u := range []string{"a", "b", "c", "d", "e", "f"} {
		members = append(members, RoomMember{RoomID: "!six:x", UserID: "@" + u + ":x", DisplayName: u, Membership: "join"})
	}
	if err := st.Upsert(ctx, SyncStats{}, rooms, members, nil); err != nil {
		t.Fatal(err)
	}
	labels, err := st.RoomLabels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"!named:x": "Named",
		"!alias:x": "#alias:x",
		"!dm:x":    "Alice",
		"!group:x": "Alice and @bob:x",
		"!big:x":   "a, b, c, d, e and 2 others",
		"!six:x":   "a, b, c, d, e and 1 other",
		"!empty:x": "Empty room (was Dave)",
	}
	for id, w := range want {
		if labels[id] != w {
			t.Errorf("label[%s] = %q, want %q", id, labels[id], w)
		}
	}
	if _, ok := labels["!nothing:x"]; ok {
		t.Errorf("room with no members should be unlabeled, got %q", labels["!nothing:x"])
	}
}

func TestSetDirectRooms(t *testing.T) {
	st, ctx := openTestStore(t)
	if err := st.Upsert(ctx, SyncStats{}, []Room{{ID: "!a:x"}, {ID: "!b:x"}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.SetDirectRooms(ctx, []string{"!a:x"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetDirectRooms(ctx, []string{"!b:x"}); err != nil {
		t.Fatal(err)
	}
	rooms, _ := st.ListRooms(ctx, RoomFilter{Limit: 10})
	for _, r := range rooms {
		if r.IsDirect != (r.ID == "!b:x") {
			t.Errorf("%s is_direct = %v", r.ID, r.IsDirect)
		}
	}
}

func TestMessagesResolveSenderAndRoomNames(t *testing.T) {
	st, ctx := openTestStore(t)
	rooms := []Room{{ID: "!a:x", Name: "Ops"}, {ID: "!b:x"}}
	members := []RoomMember{
		{RoomID: "!a:x", UserID: "@alice:x", DisplayName: "Alice (Ops)", Membership: "join"},
		{RoomID: "!b:x", UserID: "@bob:x", DisplayName: "Bob", Membership: "join"},
	}
	ts := time.Unix(1_700_000_000, 0).UTC()
	messages := []Message{
		{RoomID: "!a:x", EventID: "$1", Sender: "@alice:x", OriginServerTS: ts, MsgType: "m.text", Body: "hi"},
		{RoomID: "!a:x", EventID: "$2", Sender: "@bob:x", OriginServerTS: ts.Add(time.Second), MsgType: "m.text", Body: "yo"},
		{RoomID: "!a:x", EventID: "$3", Sender: "@carol:x", OriginServerTS: ts.Add(2 * time.Second), MsgType: "m.text", Body: "?"},
	}
	if err := st.Upsert(ctx, SyncStats{}, rooms, members, messages); err != nil {
		t.Fatal(err)
	}
	got, err := st.Messages(ctx, MessageFilter{RoomID: "!a:x", Asc: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Alice (Ops)", "Bob", ""} // bob: from another room; carol: unknown
	for i, m := range got {
		if m.SenderDisplayName != want[i] || m.RoomName != "Ops" {
			t.Errorf("%s: sender=%q room=%q", m.EventID, m.SenderDisplayName, m.RoomName)
		}
	}
}
