package matrix

import (
	"context"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"

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
func SyncOnce(ctx context.Context, client *mautrix.Client, st *store.Store) (store.SyncStats, error) {
	started := time.Now().UTC()
	since, err := st.GetSyncState(ctx, "next_batch")
	if err != nil {
		return store.SyncStats{}, err
	}
	fullState := since == ""

	resp, err := client.SyncRequest(ctx, syncTimeoutMS, since, "", fullState, event.PresenceOnline)
	if err != nil {
		return store.SyncStats{}, err
	}

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
		for _, e := range joined.Timeline.Events {
			sa.apply(e)
			ma.apply(e)
			if msg, ok := eventToMessage(roomID, e); ok {
				messages = append(messages, msg)
			}
		}

		room := sa.result()
		if pb := trimToken(joined.Timeline.PrevBatch); pb != "" {
			room.PrevBatch = pb
		}
		if joined.Summary.JoinedMemberCount != nil {
			room.MemberCount = *joined.Summary.JoinedMemberCount
		}
		rooms = append(rooms, room)
		members = append(members, ma.result()...)
	}

	stats := store.SyncStats{
		DBPath:     st.Path(),
		Rooms:      len(rooms),
		Messages:   len(messages),
		NextBatch:  resp.NextBatch,
		StartedAt:  started,
		FinishedAt: time.Now().UTC(),
	}

	if err := st.Upsert(ctx, stats, rooms, members, messages); err != nil {
		return stats, err
	}
	return stats, nil
}
