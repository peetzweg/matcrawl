package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// EventRef identifies an event within a room.
type EventRef struct {
	RoomID  string `json:"room_id"`
	EventID string `json:"event_id"`
}

// Batch is everything one /sync response, backfill page, or decrypt pass
// contributes to the archive. Apply writes it atomically.
type Batch struct {
	Rooms         []Room
	Members       []RoomMember
	Messages      []Message
	Edits         []Edit
	Reactions     []Reaction
	Redactions    []Redaction
	SpaceChildren []SpaceChild
	// DropMessages removes rows that turned out not to be messages, e.g. an
	// encrypted placeholder that decrypts to a reaction or an edit.
	DropMessages []EventRef
	// SyncState is written in the same transaction, for pagination
	// checkpoints that must not outrun the events they cover.
	SyncState map[string]string
}

// Edit is an m.replace event: a new version of TargetEventID's content.
type Edit struct {
	RoomID         string    `json:"room_id"`
	EventID        string    `json:"event_id"`
	TargetEventID  string    `json:"target_event_id"`
	Sender         string    `json:"sender"`
	OriginServerTS time.Time `json:"origin_server_ts"`
	MsgType        string    `json:"msgtype,omitempty"`
	Body           string    `json:"body,omitempty"`
	FormattedBody  string    `json:"formatted_body,omitempty"`
	WasEncrypted   bool      `json:"was_encrypted,omitempty"`
	RawJSON        string    `json:"raw_json,omitempty"`
}

// Reaction is an m.annotation (emoji reaction) on TargetEventID.
type Reaction struct {
	RoomID         string    `json:"room_id"`
	EventID        string    `json:"event_id"`
	TargetEventID  string    `json:"target_event_id"`
	Sender         string    `json:"sender"`
	Key            string    `json:"key"`
	OriginServerTS time.Time `json:"origin_server_ts"`
	WasEncrypted   bool      `json:"was_encrypted,omitempty"`
}

// Redaction is a deletion of TargetEventID.
type Redaction struct {
	RoomID         string    `json:"room_id"`
	EventID        string    `json:"event_id"`
	TargetEventID  string    `json:"target_event_id"`
	Sender         string    `json:"sender"`
	OriginServerTS time.Time `json:"origin_server_ts"`
	Reason         string    `json:"reason,omitempty"`
}

// SpaceChild is an m.space.child state event. Removed is set when the event
// has empty content, which is how a child is taken out of a space.
type SpaceChild struct {
	SpaceID   string   `json:"space_id"`
	ChildID   string   `json:"child_id"`
	Suggested bool     `json:"suggested,omitempty"`
	Order     string   `json:"order,omitempty"`
	Via       []string `json:"via,omitempty"`
	Removed   bool     `json:"-"`
}

// EditRevision is one version of an edited message, oldest first.
type EditRevision struct {
	EventID        string    `json:"event_id"`
	OriginServerTS time.Time `json:"origin_server_ts"`
	Body           string    `json:"body"`
}

// ReactionCount aggregates one reaction key on a message.
type ReactionCount struct {
	Key     string   `json:"key"`
	Count   int      `json:"count"`
	Senders []string `json:"senders"`
}

// RedactionInfo marks a deleted message. The archive keeps whatever content
// it captured before the deletion.
type RedactionInfo struct {
	EventID        string    `json:"event_id"`
	By             string    `json:"by"`
	OriginServerTS time.Time `json:"origin_server_ts"`
	Reason         string    `json:"reason,omitempty"`
}

func applyRelations(ctx context.Context, tx *sql.Tx, b Batch, reindex map[EventRef]struct{}, touched map[string]struct{}) error {
	for _, e := range b.Edits {
		if _, err := tx.ExecContext(ctx, `
			insert into edits(room_id,event_id,target_event_id,sender,origin_server_ts,msgtype,body,formatted_body,was_encrypted,raw_json)
			values(?,?,?,?,?,?,?,?,?,?)
			on conflict(room_id,event_id) do update set
				target_event_id=excluded.target_event_id, sender=excluded.sender,
				origin_server_ts=excluded.origin_server_ts, msgtype=excluded.msgtype,
				body=excluded.body, formatted_body=excluded.formatted_body,
				was_encrypted=excluded.was_encrypted, raw_json=excluded.raw_json
		`, e.RoomID, e.EventID, e.TargetEventID, e.Sender, unix(e.OriginServerTS), e.MsgType, e.Body, e.FormattedBody, boolInt(e.WasEncrypted), e.RawJSON); err != nil {
			return fmt.Errorf("upsert edit %s/%s: %w", e.RoomID, e.EventID, err)
		}
		reindex[EventRef{e.RoomID, e.TargetEventID}] = struct{}{}
	}

	for _, r := range b.Reactions {
		if _, err := tx.ExecContext(ctx, `
			insert into reactions(room_id,event_id,target_event_id,sender,key,origin_server_ts,was_encrypted)
			values(?,?,?,?,?,?,?)
			on conflict(room_id,event_id) do update set
				target_event_id=excluded.target_event_id, sender=excluded.sender, key=excluded.key,
				origin_server_ts=excluded.origin_server_ts, was_encrypted=excluded.was_encrypted
		`, r.RoomID, r.EventID, r.TargetEventID, r.Sender, r.Key, unix(r.OriginServerTS), boolInt(r.WasEncrypted)); err != nil {
			return fmt.Errorf("upsert reaction %s/%s: %w", r.RoomID, r.EventID, err)
		}
	}

	for _, r := range b.Redactions {
		if _, err := tx.ExecContext(ctx, `
			insert into redactions(room_id,event_id,target_event_id,sender,origin_server_ts,reason)
			values(?,?,?,?,?,?)
			on conflict(room_id,event_id) do update set
				target_event_id=excluded.target_event_id, sender=excluded.sender,
				origin_server_ts=excluded.origin_server_ts, reason=excluded.reason
		`, r.RoomID, r.EventID, r.TargetEventID, r.Sender, unix(r.OriginServerTS), r.Reason); err != nil {
			return fmt.Errorf("upsert redaction %s/%s: %w", r.RoomID, r.EventID, err)
		}
		// Deleting an edit changes its target's current text.
		var target string
		err := tx.QueryRowContext(ctx, `select target_event_id from edits where room_id = ? and event_id = ?`, r.RoomID, r.TargetEventID).Scan(&target)
		switch {
		case err == nil:
			reindex[EventRef{r.RoomID, target}] = struct{}{}
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
	}

	for _, c := range b.SpaceChildren {
		if c.Removed {
			if _, err := tx.ExecContext(ctx, `delete from space_children where space_id = ? and child_id = ?`, c.SpaceID, c.ChildID); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			insert into space_children(space_id,child_id,suggested,order_key,via_json)
			values(?,?,?,?,?)
			on conflict(space_id,child_id) do update set
				suggested=excluded.suggested, order_key=excluded.order_key, via_json=excluded.via_json
		`, c.SpaceID, c.ChildID, boolInt(c.Suggested), c.Order, jsonString(c.Via)); err != nil {
			return fmt.Errorf("upsert space child %s/%s: %w", c.SpaceID, c.ChildID, err)
		}
	}

	for _, ref := range b.DropMessages {
		if _, err := tx.ExecContext(ctx, `delete from messages_fts where rowid = (select rowid from messages where room_id = ? and event_id = ?)`, ref.RoomID, ref.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `delete from messages where room_id = ? and event_id = ?`, ref.RoomID, ref.EventID); err != nil {
			return err
		}
		delete(reindex, ref)
		touched[ref.RoomID] = struct{}{}
	}
	return nil
}

// currentBodySQL is a message's text as it reads now: the newest edit by the
// original sender that hasn't itself been deleted, else the original body.
const currentBodySQL = `coalesce(
	(select e.body from edits e
	 where e.room_id = m.room_id and e.target_event_id = m.event_id and e.sender = m.sender
	   and not exists (select 1 from redactions x where x.room_id = e.room_id and x.target_event_id = e.event_id)
	 order by e.origin_server_ts desc limit 1),
	m.body, '')`

// reindexFTS rebuilds one message's FTS row from its current body. FTS5 has
// no ON CONFLICT support, so the row is deleted and re-inserted.
func reindexFTS(ctx context.Context, tx *sql.Tx, ref EventRef) error {
	var rowid int64
	var m Message
	err := tx.QueryRowContext(ctx, `
		select m.rowid, `+currentBodySQL+`, m.sender, coalesce(m.sender_display_name,''), coalesce(m.decrypt_status,'')
		from messages m where m.room_id = ? and m.event_id = ?
	`, ref.RoomID, ref.EventID).Scan(&rowid, &m.Body, &m.Sender, &m.SenderDisplayName, &m.DecryptStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // target not archived yet; indexed when it arrives
	}
	if err != nil {
		return fmt.Errorf("fts lookup %s/%s: %w", ref.RoomID, ref.EventID, err)
	}
	if _, err := tx.ExecContext(ctx, `delete from messages_fts where rowid = ?`, rowid); err != nil {
		return fmt.Errorf("fts delete: %w", err)
	}
	if !ftsIndexable(m) {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `insert into messages_fts(rowid,body,room,sender) values(?,?,?,?)`,
		rowid, strings.TrimSpace(m.Body), ref.RoomID, displayOrID(m.SenderDisplayName, m.Sender)); err != nil {
		return fmt.Errorf("fts insert: %w", err)
	}
	return nil
}

// enrichMessages fills the read-time relation fields (edits, reactions,
// redaction) on msgs in place.
func (s *Store) enrichMessages(ctx context.Context, msgs []Message) error {
	byRoom := map[string][]int{}
	for i, m := range msgs {
		byRoom[m.RoomID] = append(byRoom[m.RoomID], i)
	}
	for roomID, idxs := range byRoom {
		for start := 0; start < len(idxs); start += 400 {
			chunk := idxs[start:min(start+400, len(idxs))]
			if err := s.enrichChunk(ctx, roomID, msgs, chunk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) enrichChunk(ctx context.Context, roomID string, msgs []Message, idxs []int) error {
	byEvent := make(map[string]*Message, len(idxs))
	args := []any{roomID}
	for _, i := range idxs {
		byEvent[msgs[i].EventID] = &msgs[i]
		args = append(args, msgs[i].EventID)
	}
	in := "(" + strings.TrimSuffix(strings.Repeat("?,", len(idxs)), ",") + ")"

	rows, err := s.db.QueryContext(ctx, `
		select e.target_event_id, e.event_id, e.sender, e.origin_server_ts, coalesce(e.body,''), coalesce(e.formatted_body,'')
		from edits e
		where e.room_id = ? and e.target_event_id in `+in+`
		  and not exists (select 1 from redactions x where x.room_id = e.room_id and x.target_event_id = e.event_id)
		order by e.origin_server_ts asc
	`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var target, eventID, sender, body, formatted string
		var ts int64
		if err := rows.Scan(&target, &eventID, &sender, &ts, &body, &formatted); err != nil {
			_ = rows.Close()
			return err
		}
		m := byEvent[target]
		if m == nil || sender != m.Sender { // only the original sender may edit
			continue
		}
		if m.OriginalBody == "" {
			m.OriginalBody = m.Body
		}
		m.Edits = append(m.Edits, EditRevision{EventID: eventID, OriginServerTS: fromUnix(ts), Body: body})
		m.Body, m.FormattedBody, m.EditedAt = body, formatted, fromUnix(ts)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	rows, err = s.db.QueryContext(ctx, `
		select r.target_event_id, r.key, r.sender
		from reactions r
		where r.room_id = ? and r.target_event_id in `+in+`
		  and not exists (select 1 from redactions x where x.room_id = r.room_id and x.target_event_id = r.event_id)
		order by r.origin_server_ts asc
	`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var target, key, sender string
		if err := rows.Scan(&target, &key, &sender); err != nil {
			_ = rows.Close()
			return err
		}
		m := byEvent[target]
		if m == nil {
			continue
		}
		addReaction(m, key, sender)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	rows, err = s.db.QueryContext(ctx, `
		select target_event_id, event_id, sender, origin_server_ts, coalesce(reason,'')
		from redactions
		where room_id = ? and target_event_id in `+in+`
		order by origin_server_ts asc
	`, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var target string
		var info RedactionInfo
		var ts int64
		if err := rows.Scan(&target, &info.EventID, &info.By, &ts, &info.Reason); err != nil {
			return err
		}
		if m := byEvent[target]; m != nil && m.Redacted == nil {
			info.OriginServerTS = fromUnix(ts)
			m.Redacted = &info
		}
	}
	return rows.Err()
}

// addReaction counts one sender once per key, as clients do.
func addReaction(m *Message, key, sender string) {
	for i := range m.Reactions {
		if m.Reactions[i].Key != key {
			continue
		}
		for _, s := range m.Reactions[i].Senders {
			if s == sender {
				return
			}
		}
		m.Reactions[i].Count++
		m.Reactions[i].Senders = append(m.Reactions[i].Senders, sender)
		return
	}
	m.Reactions = append(m.Reactions, ReactionCount{Key: key, Count: 1, Senders: []string{sender}})
}

func jsonString(v []string) string {
	if len(v) == 0 {
		return ""
	}
	buf, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(buf)
}

// HasAnyEvent reports whether any of eventIDs is already archived in roomID,
// as a message or a relation. Gap filling stops once it reaches known history.
func (s *Store) HasAnyEvent(ctx context.Context, roomID string, eventIDs []string) (bool, error) {
	if len(eventIDs) == 0 {
		return false, nil
	}
	in := "(" + strings.TrimSuffix(strings.Repeat("?,", len(eventIDs)), ",") + ")"
	args := make([]any, 0, len(eventIDs)+1)
	args = append(args, roomID)
	for _, id := range eventIDs {
		args = append(args, id)
	}
	for _, table := range []string{"messages", "edits", "reactions", "redactions"} {
		var n int
		if err := s.db.QueryRowContext(ctx, `select count(*) from `+table+` where room_id = ? and event_id in `+in, args...).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}

// PlaintextRawMessages returns stored plaintext messages whose raw event JSON
// contains any of the given substrings, for re-classifying history after the
// event model learns a new relation type.
func (s *Store) PlaintextRawMessages(ctx context.Context, contains ...string) ([]Message, error) {
	if len(contains) == 0 {
		return nil, nil
	}
	var conds []string
	var args []any
	for _, c := range contains {
		conds = append(conds, "instr(raw_json, ?) > 0")
		args = append(args, c)
	}
	rows, err := s.db.QueryContext(ctx, `
		select room_id, event_id, raw_json from messages
		where was_encrypted = 0 and coalesce(raw_json,'') <> '' and (`+strings.Join(conds, " or ")+`)
	`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.RoomID, &m.EventID, &m.RawJSON); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
