package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/peetzweg/matcrawl/internal/store"
)

func TestPushPullRoundTrip(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	src, err := store.Open(ctx, filepath.Join(tmp, "src.db"))
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer func() { _ = src.Close() }()

	now := time.Unix(1_700_000_000, 0).UTC()
	rooms := []store.Room{{
		ID:          "!room:example.org",
		Name:        "Roundtrip",
		IsEncrypted: false,
		LastEventTS: now,
	}}
	members := []store.RoomMember{{
		RoomID:     "!room:example.org",
		UserID:     "@a:example.org",
		Membership: "join",
		PowerLevel: 50,
	}}
	messages := []store.Message{
		{
			RoomID:         "!room:example.org",
			EventID:        "$evt-2024-01",
			Sender:         "@a:example.org",
			OriginServerTS: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			MsgType:        "m.text",
			Body:           "from january",
		},
		{
			RoomID:         "!room:example.org",
			EventID:        "$evt-2024-02",
			Sender:         "@b:example.org",
			OriginServerTS: time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC),
			MsgType:        "m.text",
			Body:           "from february",
		},
	}
	if err := src.Upsert(ctx, store.SyncStats{FinishedAt: now}, rooms, members, messages); err != nil {
		t.Fatalf("seed: %v", err)
	}
	relations := store.Batch{
		Edits: []store.Edit{{RoomID: "!room:example.org", EventID: "$edit", TargetEventID: "$evt-2024-01", Sender: "@a:example.org",
			OriginServerTS: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC), Body: "edited in january"}},
		Reactions: []store.Reaction{{RoomID: "!room:example.org", EventID: "$react", TargetEventID: "$evt-2024-02", Sender: "@a:example.org",
			Key: "👍", OriginServerTS: time.Date(2024, 2, 21, 0, 0, 0, 0, time.UTC)}},
		Redactions: []store.Redaction{{RoomID: "!room:example.org", EventID: "$del", TargetEventID: "$evt-gone", Sender: "@a:example.org",
			OriginServerTS: time.Date(2024, 2, 22, 0, 0, 0, 0, time.UTC)}},
		SpaceChildren: []store.SpaceChild{{SpaceID: "!room:example.org", ChildID: "!child:example.org", Via: []string{"example.org"}}},
	}
	if err := src.Apply(ctx, store.SyncStats{FinishedAt: now}, relations); err != nil {
		t.Fatalf("seed relations: %v", err)
	}

	repo := filepath.Join(tmp, "repo")
	identity := filepath.Join(tmp, "age.key")
	configPath := filepath.Join(tmp, "backup.json")

	if _, _, err := Init(ctx, Options{
		ConfigPath: configPath,
		Repo:       repo,
		Identity:   identity,
		Push:       false,
	}); err != nil {
		t.Fatalf("init: %v", err)
	}

	pushed, err := Push(ctx, src, Options{
		ConfigPath: configPath,
		Repo:       repo,
		Identity:   identity,
		Push:       false,
	})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if !pushed.Changed || pushed.Messages != 2 {
		t.Errorf("push result = %+v, want Changed=true Messages=2", pushed)
	}

	for _, want := range []string{
		filepath.Join(repo, "manifest.json"),
		filepath.Join(repo, "data", "rooms.jsonl.gz.age"),
		filepath.Join(repo, "data", "members.jsonl.gz.age"),
		filepath.Join(repo, "data", "messages", "2024", "01.jsonl.gz.age"),
		filepath.Join(repo, "data", "messages", "2024", "02.jsonl.gz.age"),
		filepath.Join(repo, "data", "edits.jsonl.gz.age"),
		filepath.Join(repo, "data", "reactions.jsonl.gz.age"),
		filepath.Join(repo, "data", "redactions.jsonl.gz.age"),
		filepath.Join(repo, "data", "space_children.jsonl.gz.age"),
	} {
		if _, err := os.Stat(want); err != nil {
			t.Errorf("missing artifact %s: %v", want, err)
		}
	}

	dst, err := store.Open(ctx, filepath.Join(tmp, "dst.db"))
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer func() { _ = dst.Close() }()

	pulled, err := Pull(ctx, dst, Options{
		ConfigPath: configPath,
		Repo:       repo,
		Identity:   identity,
	})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if pulled.Messages != 2 {
		t.Errorf("pulled messages = %d, want 2", pulled.Messages)
	}

	st, err := dst.Status(ctx)
	if err != nil {
		t.Fatalf("dst status: %v", err)
	}
	if st.Rooms != 1 || st.Messages != 2 {
		t.Errorf("dst status = %+v, want rooms=1 messages=2", st)
	}

	if st.Edits != 1 || st.Reactions != 1 || st.Redactions != 1 {
		t.Errorf("dst relations = %+v, want one each", st)
	}

	// The restored index searches the edited text.
	hits, err := dst.Search(ctx, store.MessageFilter{Query: "edited"})
	if err != nil {
		t.Fatalf("dst search: %v", err)
	}
	if len(hits) != 1 || hits[0].EventID != "$evt-2024-01" || hits[0].OriginalBody != "from january" {
		t.Errorf("FTS search on restored store returned %+v", hits)
	}
}
