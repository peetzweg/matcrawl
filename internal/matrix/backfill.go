package matrix

import (
	"context"
	"fmt"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"

	"github.com/peetzweg/matcrawl/internal/store"
)

const (
	messagesPerPage    = 100
	messagesPageTimout = 5 * time.Minute
)

// BackfillResult summarises a per-room backfill pass.
type BackfillResult struct {
	RoomID   string `json:"room_id"`
	Pages    int    `json:"pages"`
	Messages int    `json:"messages"`
	Done     bool   `json:"done"`
}

// BackfillRoom walks a room backwards from its persisted prev_batch token
// using /rooms/{id}/messages?dir=b until exhaustion (resp.End == "" or
// resp.End == resp.Start). Checkpoints after every page so a Ctrl-C resumes
// from the same spot.
//
// Honors mautrix's built-in 429/retry_after_ms handling: by default the
// inner http client sleeps for the server-advertised duration before
// retrying, so we don't need an extra retry loop here (see MATCRAWL.md §4.5).
func BackfillRoom(ctx context.Context, client *mautrix.Client, st *store.Store, room store.Room) (BackfillResult, error) {
	res := BackfillResult{RoomID: room.ID}
	from := trimToken(room.PrevBatch)
	if from == "" {
		res.Done = true
		return res, nil
	}

	for from != "" {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		callCtx, cancel := context.WithTimeout(ctx, messagesPageTimout)
		resp, err := client.Messages(callCtx, id.RoomID(room.ID), from, "", mautrix.DirectionBackward, nil, messagesPerPage)
		cancel()
		if err != nil {
			return res, fmt.Errorf("messages %s from %q: %w", room.ID, from, err)
		}
		res.Pages++

		var batch []store.Message
		for _, e := range resp.Chunk {
			if msg, ok := eventToMessage(id.RoomID(room.ID), e); ok {
				batch = append(batch, msg)
			}
		}

		end := trimToken(resp.End)
		room.PrevBatch = end

		if err := st.Upsert(ctx, store.SyncStats{
			DBPath:     st.Path(),
			Rooms:      1,
			Messages:   len(batch),
			FinishedAt: time.Now().UTC(),
		}, []store.Room{room}, nil, batch); err != nil {
			return res, err
		}
		res.Messages += len(batch)

		if end == "" || end == trimToken(resp.Start) {
			res.Done = true
			return res, nil
		}
		from = end
	}
	return res, nil
}

// BackfillAll iterates the joined rooms in the store and runs BackfillRoom
// for each. Stops on first error; returns the partial results so the caller
// can surface them.
func BackfillAll(ctx context.Context, client *mautrix.Client, st *store.Store) ([]BackfillResult, error) {
	rooms, err := st.ListRooms(ctx, store.RoomFilter{Limit: 1_000_000})
	if err != nil {
		return nil, err
	}
	var results []BackfillResult
	for _, r := range rooms {
		if r.PrevBatch == "" {
			results = append(results, BackfillResult{RoomID: r.ID, Done: true})
			continue
		}
		res, err := BackfillRoom(ctx, client, st, r)
		results = append(results, res)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}
