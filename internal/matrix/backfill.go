package matrix

import (
	"context"
	"fmt"
	"io"
	"strings"
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
	label := roomLabel(room)
	res := BackfillResult{RoomID: room.ID, Name: room.DisplayName}
	from := trimToken(room.PrevBatch)
	if from == "" {
		res.Done = true
		logf(progress, "backfill: %s already exhausted (no prev_batch)\n", label)
		return res, nil
	}
	logf(progress, "backfill: %s starting from %s\n", label, short(from))
	err := walkBack(ctx, client, st, room.ID, from, &res, label, progress, func(b *store.Batch, end, _ string) {
		// Only the cursor: the room row was loaded before this walk began
		// and must not overwrite fresher state.
		b.Rooms = []store.Room{{ID: room.ID, PrevBatch: end}}
	})
	return res, err
}

// historyScanVersion bumps when the archive starts capturing event types it
// used to drop. Each room is then walked once more from its newest event to
// its first. v1: edits, reactions, redactions, stickers and polls.
const historyScanVersion = "1"

func historyScanKey(roomID string) string { return "history_scan:" + roomID }

// ScanRoomHistory re-walks a whole room from now back to its creation,
// resuming from its checkpoint. On completion it records the scan version
// so later backfills skip it.
func ScanRoomHistory(ctx context.Context, client *mautrix.Client, st *store.Store, room store.Room, progress io.Writer) (BackfillResult, error) {
	label := roomLabel(room)
	res := BackfillResult{RoomID: room.ID, Name: room.DisplayName}
	state, err := st.GetSyncState(ctx, historyScanKey(room.ID))
	if err != nil {
		return res, err
	}
	// Empty "from" asks the server to start at the newest event.
	from, _ := strings.CutPrefix(state, "at:")
	if !strings.HasPrefix(state, "at:") {
		from = ""
	}
	logf(progress, "backfill: %s rescanning history (v%s) from %s\n", label, historyScanVersion, orLatest(from))
	err = walkBack(ctx, client, st, room.ID, from, &res, label, progress, func(b *store.Batch, end, last string) {
		if end == "" {
			// Reached the room's start: park the backfill cursor on the
			// final page so plain backfills see the room as exhausted.
			b.SyncState = map[string]string{historyScanKey(room.ID): "done:" + historyScanVersion}
			if last != "" {
				b.Rooms = []store.Room{{ID: room.ID, PrevBatch: last}}
			}
			return
		}
		b.SyncState = map[string]string{historyScanKey(room.ID): "at:" + end}
		b.Rooms = []store.Room{{ID: room.ID, PrevBatch: end}}
	})
	return res, err
}

// walkBack pages backwards from `from` until the room's start. checkpoint
// adds the resume cursor to each page's batch ("" once the walk is
// complete, with last set to the final page's token), so the cursor is
// committed atomically with the events.
func walkBack(ctx context.Context, client *mautrix.Client, st *store.Store, roomID, from string, res *BackfillResult, label string, progress io.Writer, checkpoint func(b *store.Batch, end, last string)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageStart := time.Now()
		callCtx, cancel := context.WithTimeout(ctx, messagesPageTimout)
		resp, err := client.Messages(callCtx, id.RoomID(roomID), from, "", mautrix.DirectionBackward, nil, messagesPerPage)
		cancel()
		if err != nil {
			return fmt.Errorf("messages %s from %q: %w", roomID, from, err)
		}
		res.Pages++

		var b store.Batch
		for _, e := range resp.Chunk {
			appendEvent(&b, id.RoomID(roomID), e)
		}
		end := trimToken(resp.End)
		done := end == "" || end == trimToken(resp.Start)
		if done {
			checkpoint(&b, "", from)
		} else {
			checkpoint(&b, end, from)
		}
		if err := st.Apply(ctx, store.SyncStats{
			DBPath:     st.Path(),
			Rooms:      1,
			Messages:   len(b.Messages),
			FinishedAt: time.Now().UTC(),
		}, b); err != nil {
			return err
		}
		res.Messages += len(b.Messages)
		logf(progress, "backfill:   %s page=%d events=%d (msgs=%d edits=%d reactions=%d redactions=%d) (%.1fs) next=%s\n",
			label, res.Pages, len(resp.Chunk), len(b.Messages), len(b.Edits), len(b.Reactions), len(b.Redactions),
			time.Since(pageStart).Seconds(), short(end))
		if done {
			res.Done = true
			logf(progress, "backfill: %s done, %d messages across %d pages\n", label, res.Messages, res.Pages)
			return nil
		}
		from = end
	}
}

// BackfillAll iterates the rooms in the store. A room whose history hasn't
// been scanned at the current historyScanVersion is rescanned in full;
// otherwise it continues backwards from its prev_batch. By default it
// continues past per-room errors (a single slow or broken room shouldn't
// stall the rest of the archive). Set stopOnError to abort on first failure.
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
		label := roomLabel(r)
		logf(progress, "backfill: [%d/%d] %s\n", i+1, len(rooms), label)
		scan, err := st.GetSyncState(ctx, historyScanKey(r.ID))
		if err != nil {
			return results, err
		}
		var res BackfillResult
		switch {
		case scan != "done:"+historyScanVersion:
			res, err = ScanRoomHistory(ctx, client, st, r, progress)
		case r.PrevBatch == "":
			results = append(results, BackfillResult{RoomID: r.ID, Name: r.DisplayName, Done: true})
			continue
		default:
			res, err = BackfillRoom(ctx, client, st, r, progress)
		}
		if err != nil {
			if ctx.Err() != nil {
				return results, ctx.Err()
			}
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

func roomLabel(r store.Room) string {
	if r.DisplayName != "" {
		return r.DisplayName
	}
	return r.ID
}

func orLatest(token string) string {
	if token == "" {
		return "latest"
	}
	return short(token)
}
