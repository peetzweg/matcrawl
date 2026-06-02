package matrix

import (
	"context"
	"fmt"
	"io"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/peetzweg/matcrawl/internal/store"
)

const syncTimeoutMS = 30_000

// SyncOnce runs a single /sync against the homeserver and persists the
// resulting room state + timeline events into the store. The next_batch
// token is checkpointed in sync_state so the next invocation resumes from
// the same point. On the first call (no persisted next_batch), fullState is
// set so the room list comes back populated.
//
// Encrypted (m.room.encrypted) events are recorded with DecryptStatus =
// "missing_keys" — PR 5 (cryptohelper wiring) upgrades them in place.
//
// progress is an optional stderr-style writer; pass nil to suppress logging.
func SyncOnce(ctx context.Context, client *mautrix.Client, st *store.Store, progress io.Writer) (store.SyncStats, error) {
	started := time.Now().UTC()
	since, err := st.GetSyncState(ctx, "next_batch")
	if err != nil {
		return store.SyncStats{}, err
	}
	fullState := since == ""

	logf(progress, "sync: requesting /sync (since=%q full_state=%v timeout=%dms)\n", short(since), fullState, syncTimeoutMS)
	resp, err := client.SyncRequest(ctx, syncTimeoutMS, since, "", fullState, event.PresenceOnline)
	if err != nil {
		return store.SyncStats{}, err
	}
	logf(progress, "sync: /sync returned (join=%d leave=%d invite=%d)\n",
		len(resp.Rooms.Join), len(resp.Rooms.Leave), len(resp.Rooms.Invite))

	var (
		rooms    []store.Room
		members  []store.RoomMember
		messages []store.Message
	)

	for roomID, joined := range resp.Rooms.Join {
		sa := newStateAccumulator(roomID)
		ma := newMemberAccumulator(roomID)

		for _, e := range joined.State.Events {
			sa.apply(e)
			ma.apply(e)
		}
		var timelineMessages int
		for _, e := range joined.Timeline.Events {
			sa.apply(e)
			ma.apply(e)
			if msg, ok := eventToMessage(roomID, e); ok {
				messages = append(messages, msg)
				timelineMessages++
			}
		}

		room := sa.result()
		if pb := trimToken(joined.Timeline.PrevBatch); pb != "" {
			room.PrevBatch = pb
		}
		// Prefer the server's authoritative count when it's present;
		// otherwise derive from the join-member events we saw in state.
		switch {
		case joined.Summary.JoinedMemberCount != nil:
			room.MemberCount = *joined.Summary.JoinedMemberCount
		default:
			room.MemberCount = ma.joinCount()
		}
		rooms = append(rooms, room)
		members = append(members, ma.result()...)
		logf(progress, "sync:   join %s name=%q members=%d timeline_msgs=%d prev_batch=%v\n",
			roomID, room.Name, room.MemberCount, timelineMessages, room.PrevBatch != "")
	}

	// Left/kicked rooms still belong in an archive. Capture them at "left"
	// status so the user sees them in `matcrawl rooms` even though they're
	// no longer a participant.
	for roomID, left := range resp.Rooms.Leave {
		sa := newStateAccumulator(roomID)
		ma := newMemberAccumulator(roomID)
		for _, e := range left.State.Events {
			sa.apply(e)
			ma.apply(e)
		}
		var timelineMessages int
		for _, e := range left.Timeline.Events {
			sa.apply(e)
			ma.apply(e)
			if msg, ok := eventToMessage(roomID, e); ok {
				messages = append(messages, msg)
				timelineMessages++
			}
		}
		room := sa.result()
		if pb := trimToken(left.Timeline.PrevBatch); pb != "" {
			room.PrevBatch = pb
		}
		room.MemberCount = ma.joinCount()
		rooms = append(rooms, room)
		members = append(members, ma.result()...)
		logf(progress, "sync:   left %s name=%q timeline_msgs=%d prev_batch=%v\n",
			roomID, room.Name, timelineMessages, room.PrevBatch != "")
	}

	stats := store.SyncStats{
		DBPath:     st.Path(),
		Rooms:      len(rooms),
		Messages:   len(messages),
		NextBatch:  resp.NextBatch,
		StartedAt:  started,
		FinishedAt: time.Now().UTC(),
	}

	logf(progress, "sync: upserting rooms=%d members=%d messages=%d\n", len(rooms), len(members), len(messages))
	if err := st.Upsert(ctx, stats, rooms, members, messages); err != nil {
		return stats, err
	}
	return stats, nil
}

func logf(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, format, args...)
}

func short(token string) string {
	if len(token) <= 12 {
		return token
	}
	return token[:8] + "…"
}

// ensure id package import stays live for callers building room IDs from
// stored strings (used by backfill).
var _ = id.RoomID("")
