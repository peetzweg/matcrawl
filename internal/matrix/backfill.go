package matrix

import (
	"context"
	"fmt"
	"io"
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
	Name     string `json:"name,omitempty"`
	Pages    int    `json:"pages"`
	Messages int    `json:"messages"`
	Done     bool   `json:"done"`
	Error    string `json:"error,omitempty"`
}

// BackfillRoom walks a room backwards from its persisted prev_batch token
// using /rooms/{id}/messages?dir=b until exhaustion (resp.End == "" or
// resp.End == resp.Start). Checkpoints after every page so a Ctrl-C resumes
// from the same spot.
//
// Honors mautrix's built-in 429/retry_after_ms handling: by default the
// inner http client sleeps for the server-advertised duration before
// retrying, so we don't need an extra retry loop here (see MATCRAWL.md §4.5).
func BackfillRoom(ctx context.Context, client *mautrix.Client, st *store.Store, room store.Room, progress io.Writer) (BackfillResult, error) {
	label := room.Name
	if label == "" {
		label = room.ID
	}
	res := BackfillResult{RoomID: room.ID, Name: room.Name}
	from := trimToken(room.PrevBatch)
	if from == "" {
		res.Done = true
		logf(progress, "backfill: %s already exhausted (no prev_batch)\n", label)
		return res, nil
	}

	logf(progress, "backfill: %s starting from %s\n", label, short(from))
	for from != "" {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		pageStart := time.Now()
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
		logf(progress, "backfill:   %s page=%d events=%d (%.1fs) next=%s\n",
			label, res.Pages, len(batch), time.Since(pageStart).Seconds(), short(end))

		if end == "" || end == trimToken(resp.Start) {
			res.Done = true
			logf(progress, "backfill: %s done, %d events across %d pages\n", label, res.Messages, res.Pages)
			return res, nil
		}
		from = end
	}
	return res, nil
}

// BackfillAll iterates the joined rooms in the store and runs BackfillRoom
// for each. By default it continues past per-room errors (a single slow or
// broken room shouldn't stall the rest of the archive). Set stopOnError to
// abort on first failure if you need that for scripting.
func BackfillAll(ctx context.Context, client *mautrix.Client, st *store.Store, progress io.Writer, stopOnError bool) ([]BackfillResult, error) {
	rooms, err := st.ListRooms(ctx, store.RoomFilter{Limit: 1_000_000})
	if err != nil {
		return nil, err
	}
	logf(progress, "backfill: %d rooms in archive\n", len(rooms))
	var results []BackfillResult
	for i, r := range rooms {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		label := r.Name
		if label == "" {
			label = r.ID
		}
		logf(progress, "backfill: [%d/%d] %s\n", i+1, len(rooms), label)
		if r.PrevBatch == "" {
			results = append(results, BackfillResult{RoomID: r.ID, Name: r.Name, Done: true})
			continue
		}
		res, err := BackfillRoom(ctx, client, st, r, progress)
		if err != nil {
			res.Error = err.Error()
			logf(progress, "backfill: %s FAILED: %v\n", label, err)
			results = append(results, res)
			if stopOnError {
				return results, err
			}
			continue
		}
		results = append(results, res)
	}
	return results, nil
}
