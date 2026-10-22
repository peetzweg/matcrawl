package matrix

import (
	"context"
	"encoding/json"
	"io"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/peetzweg/matcrawl/internal/store"
)

// eventModelVersion bumps when stored raw events need re-classifying.
// v1: edits used to be archived as standalone "* text" messages, and
// server-side redactions on stored events weren't recorded.
const eventModelVersion = "1"

// MigrateStoredEvents re-classifies plaintext messages already in the
// archive from their raw JSON, once per eventModelVersion. Encrypted rows
// are left to `keys retry`, which classifies on decryption.
func MigrateStoredEvents(ctx context.Context, st *store.Store, progress io.Writer) error {
	done, err := st.GetSyncState(ctx, "event_model_version")
	if err != nil || done == eventModelVersion {
		return err
	}
	rows, err := st.PlaintextRawMessages(ctx, `"m.replace"`, `"redacted_because"`)
	if err != nil {
		return err
	}
	var b store.Batch
	for _, row := range rows {
		var evt event.Event
		if err := json.Unmarshal([]byte(row.RawJSON), &evt); err != nil {
			continue
		}
		var one store.Batch
		appendEvent(&one, id.RoomID(row.RoomID), &evt)
		b.Redactions = append(b.Redactions, one.Redactions...)
		if len(one.Edits) > 0 {
			b.Edits = append(b.Edits, one.Edits...)
			b.DropMessages = append(b.DropMessages, store.EventRef{RoomID: row.RoomID, EventID: row.EventID})
		}
	}
	logf(progress, "migrate: %d stored edits moved out of messages, %d deletions recorded\n", len(b.Edits), len(b.Redactions))
	b.SyncState = map[string]string{"event_model_version": eventModelVersion}
	return st.Apply(ctx, store.SyncStats{DBPath: st.Path()}, b)
}
