package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "matcrawl.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, ctx
}

func TestUpsertAndStatus(t *testing.T) {
	st, ctx := openTestStore(t)

	roomID := "!abc:example.org"
	now := time.Unix(1_700_000_000, 0).UTC()
	rooms := []Room{{
		ID:          roomID,
		Name:        "Test Room",
		IsEncrypted: true,
		MemberCount: 2,
		JoinedAt:    now,
		LastEventTS: now,
	}}
	members := []RoomMember{{
		RoomID:     roomID,
		UserID:     "@alice:example.org",
		Membership: "join",
		PowerLevel: 100,
	}}
	messages := []Message{
		{
			RoomID:         roomID,
			EventID:        "$evt1",
			Sender:         "@alice:example.org",
			OriginServerTS: now,
			MsgType:        "m.text",
			Body:           "hello world",
		},
		{
			RoomID:          roomID,
			EventID:         "$evt2",
			Sender:          "@bob:example.org",
			OriginServerTS:  now.Add(time.Minute),
			MsgType:         "m.image",
			Body:            "image caption",
			AttachmentsJSON: `[{"mxc":"mxc://example.org/abc"}]`,
		},
		{
			RoomID:         roomID,
			EventID:        "$evt3",
			Sender:         "@alice:example.org",
			OriginServerTS: now.Add(2 * time.Minute),
			MsgType:        "m.text",
			WasEncrypted:   true,
			DecryptStatus:  "missing_keys",
			DecryptError:   "megolm session not found",
		},
	}

	stats := SyncStats{
		DBPath:     st.Path(),
		Rooms:      len(rooms),
		Messages:   len(messages),
		NextBatch:  "s100_0_0",
		FinishedAt: now.Add(5 * time.Minute),
	}
	if err := st.Upsert(ctx, stats, rooms, members, messages); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Re-upsert is idempotent.
	if err := st.Upsert(ctx, stats, rooms, members, messages); err != nil {
		t.Fatalf("upsert (repeat): %v", err)
	}

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Rooms != 1 {
		t.Errorf("rooms = %d, want 1", status.Rooms)
	}
	if status.EncryptedRooms != 1 {
		t.Errorf("encrypted_rooms = %d, want 1", status.EncryptedRooms)
	}
	if status.Messages != 3 {
		t.Errorf("messages = %d, want 3", status.Messages)
	}
	if status.DecryptFailures != 1 {
		t.Errorf("decrypt_failures = %d, want 1", status.DecryptFailures)
	}
	if status.MediaMessages != 1 {
		t.Errorf("media_messages = %d, want 1", status.MediaMessages)
	}
	if status.NextBatch != "s100_0_0" {
		t.Errorf("next_batch = %q, want s100_0_0", status.NextBatch)
	}
}

func TestSearchFTSAndDecryptFailureExclusion(t *testing.T) {
	st, ctx := openTestStore(t)

	roomID := "!room:example.org"
	now := time.Unix(1_700_000_000, 0).UTC()
	messages := []Message{
		{
			RoomID:         roomID,
			EventID:        "$plain",
			Sender:         "@a:example.org",
			OriginServerTS: now,
			MsgType:        "m.text",
			Body:           "the quick brown fox",
		},
		{
			RoomID:         roomID,
			EventID:        "$decrypted",
			Sender:         "@b:example.org",
			OriginServerTS: now.Add(time.Minute),
			MsgType:        "m.text",
			Body:           "fox jumps over the lazy dog",
			WasEncrypted:   true,
			DecryptStatus:  "ok",
		},
		{
			RoomID:         roomID,
			EventID:        "$encrypted_unreadable",
			Sender:         "@c:example.org",
			OriginServerTS: now.Add(2 * time.Minute),
			MsgType:        "m.text",
			Body:           "fox ciphertext",
			WasEncrypted:   true,
			DecryptStatus:  "missing_keys",
		},
	}

	if err := st.Upsert(ctx, SyncStats{FinishedAt: now}, nil, nil, messages); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	hits, err := st.Search(ctx, MessageFilter{Query: "fox"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %d, want 2 (plain + decrypted only)", len(hits))
	}
	for _, h := range hits {
		if h.EventID == "$encrypted_unreadable" {
			t.Errorf("ciphertext message %s leaked into FTS results", h.EventID)
		}
	}
}

func TestUpsertOverwritesFTSOnReDecrypt(t *testing.T) {
	st, ctx := openTestStore(t)

	roomID := "!room:example.org"
	now := time.Unix(1_700_000_000, 0).UTC()
	eventID := "$encrypted_event"

	// First pass: arrives encrypted, no keys yet — body must NOT land in FTS.
	if err := st.Upsert(ctx, SyncStats{FinishedAt: now}, nil, nil, []Message{{
		RoomID:         roomID,
		EventID:        eventID,
		Sender:         "@a:example.org",
		OriginServerTS: now,
		MsgType:        "m.text",
		Body:           "",
		WasEncrypted:   true,
		DecryptStatus:  "missing_keys",
	}}); err != nil {
		t.Fatalf("upsert encrypted: %v", err)
	}
	if hits, err := st.Search(ctx, MessageFilter{Query: "secret"}); err != nil {
		t.Fatalf("search before keys: %v", err)
	} else if len(hits) != 0 {
		t.Fatalf("hits before keys = %d, want 0", len(hits))
	}

	// Second pass: keys imported, message decrypted — body must now land in FTS.
	if err := st.Upsert(ctx, SyncStats{FinishedAt: now.Add(time.Hour)}, nil, nil, []Message{{
		RoomID:         roomID,
		EventID:        eventID,
		Sender:         "@a:example.org",
		OriginServerTS: now,
		MsgType:        "m.text",
		Body:           "secret payload",
		WasEncrypted:   true,
		DecryptStatus:  "ok",
	}}); err != nil {
		t.Fatalf("upsert decrypted: %v", err)
	}
	hits, err := st.Search(ctx, MessageFilter{Query: "secret"})
	if err != nil {
		t.Fatalf("search after keys: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("hits after keys = %d, want 1", len(hits))
	}
	if hits[0].EventID != eventID {
		t.Errorf("hit event_id = %q, want %q", hits[0].EventID, eventID)
	}
}

func TestListRoomsAndMembers(t *testing.T) {
	st, ctx := openTestStore(t)

	now := time.Unix(1_700_000_000, 0).UTC()
	rooms := []Room{
		{ID: "!plain:x.org", Name: "Plain", IsEncrypted: false, LastEventTS: now},
		{ID: "!enc:x.org", Name: "Enc", IsEncrypted: true, LastEventTS: now.Add(time.Hour)},
	}
	members := []RoomMember{
		{RoomID: "!enc:x.org", UserID: "@a:x.org", Membership: "join", PowerLevel: 100},
		{RoomID: "!enc:x.org", UserID: "@b:x.org", Membership: "join", PowerLevel: 50},
	}
	if err := st.Upsert(ctx, SyncStats{FinishedAt: now}, rooms, members, nil); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	all, err := st.ListRooms(ctx, RoomFilter{})
	if err != nil {
		t.Fatalf("list rooms: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("all rooms = %d, want 2", len(all))
	}

	enc, err := st.ListRooms(ctx, RoomFilter{EncryptedOnly: true})
	if err != nil {
		t.Fatalf("list encrypted rooms: %v", err)
	}
	if len(enc) != 1 || enc[0].ID != "!enc:x.org" {
		t.Errorf("encrypted-only = %+v, want only !enc:x.org", enc)
	}

	mems, err := st.ListMembers(ctx, "!enc:x.org")
	if err != nil {
		t.Fatalf("list members: %v", err)
	}
	if len(mems) != 2 {
		t.Errorf("members = %d, want 2", len(mems))
	}
	if mems[0].PowerLevel != 100 {
		t.Errorf("members not sorted by power_level desc, got %+v", mems)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	src, ctx := openTestStore(t)

	now := time.Unix(1_700_000_000, 0).UTC()
	rooms := []Room{{ID: "!room:x.org", Name: "R", LastEventTS: now}}
	members := []RoomMember{{RoomID: "!room:x.org", UserID: "@a:x.org", Membership: "join"}}
	messages := []Message{{
		RoomID:         "!room:x.org",
		EventID:        "$evt",
		Sender:         "@a:x.org",
		OriginServerTS: now,
		MsgType:        "m.text",
		Body:           "hello",
	}}
	if err := src.Upsert(ctx, SyncStats{FinishedAt: now}, rooms, members, messages); err != nil {
		t.Fatalf("seed upsert: %v", err)
	}

	snap, err := src.ExportAll(ctx)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := snap.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	dst, err := Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer func() { _ = dst.Close() }()
	if err := dst.ImportSnapshot(ctx, snap, "test", now); err != nil {
		t.Fatalf("import: %v", err)
	}

	got, err := dst.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if got.Rooms != 1 || got.Messages != 1 {
		t.Errorf("round-trip status = %+v, want rooms=1 messages=1", got)
	}
}
