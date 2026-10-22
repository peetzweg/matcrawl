package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Snapshot struct {
	SchemaVersion int          `json:"schema_version"`
	ExportedAt    time.Time    `json:"exported_at"`
	Rooms         []Room       `json:"rooms"`
	Members       []RoomMember `json:"members"`
	Messages      []Message    `json:"messages"`
	Edits         []Edit       `json:"edits,omitempty"`
	Reactions     []Reaction   `json:"reactions,omitempty"`
	Redactions    []Redaction  `json:"redactions,omitempty"`
	SpaceChildren []SpaceChild `json:"space_children,omitempty"`
}

func (s *Store) ExportAll(ctx context.Context) (Snapshot, error) {
	out := Snapshot{SchemaVersion: schemaVersion, ExportedAt: time.Now().UTC()}

	rooms, err := s.ListRooms(ctx, RoomFilter{Limit: 1_000_000})
	if err != nil {
		return out, fmt.Errorf("export rooms: %w", err)
	}
	out.Rooms = rooms

	memberRows, err := s.db.QueryContext(ctx, `
		select room_id, user_id, coalesce(display_name,''), coalesce(avatar_mxc,''), membership, power_level
		from room_members order by room_id asc, user_id asc
	`)
	if err != nil {
		return out, fmt.Errorf("export members: %w", err)
	}
	for memberRows.Next() {
		var m RoomMember
		if err := memberRows.Scan(&m.RoomID, &m.UserID, &m.DisplayName, &m.AvatarMXC, &m.Membership, &m.PowerLevel); err != nil {
			_ = memberRows.Close()
			return out, err
		}
		out.Members = append(out.Members, m)
	}
	if err := memberRows.Err(); err != nil {
		_ = memberRows.Close()
		return out, err
	}
	_ = memberRows.Close()

	rows, err := s.db.QueryContext(ctx, `
		select room_id, event_id, sender, coalesce(sender_display_name,''),
		       origin_server_ts, msgtype, coalesce(body,''), coalesce(formatted_body,''),
		       was_encrypted, coalesce(decrypt_status,''), coalesce(decrypt_error,''),
		       coalesce(attachments_json,''),
		       coalesce(thread_id,''), coalesce(reply_to_event_id,''), coalesce(raw_json,'')
		from messages order by origin_server_ts asc
	`)
	if err != nil {
		return out, fmt.Errorf("export messages: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var m Message
		var ts int64
		var wasEnc int
		if err := rows.Scan(
			&m.RoomID, &m.EventID, &m.Sender, &m.SenderDisplayName,
			&ts, &m.MsgType, &m.Body, &m.FormattedBody,
			&wasEnc, &m.DecryptStatus, &m.DecryptError,
			&m.AttachmentsJSON,
			&m.ThreadID, &m.ReplyToEventID, &m.RawJSON,
		); err != nil {
			return out, err
		}
		m.OriginServerTS = fromUnix(ts)
		m.WasEncrypted = wasEnc != 0
		out.Messages = append(out.Messages, m)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if err := s.exportRelations(ctx, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) exportRelations(ctx context.Context, out *Snapshot) error {
	if err := scanAll(ctx, s.db, `
		select room_id, event_id, target_event_id, sender, origin_server_ts, coalesce(msgtype,''), coalesce(body,''), coalesce(formatted_body,''), was_encrypted, coalesce(raw_json,'')
		from edits order by origin_server_ts, event_id`, func(rows *sql.Rows) error {
		var e Edit
		var ts int64
		var enc int
		if err := rows.Scan(&e.RoomID, &e.EventID, &e.TargetEventID, &e.Sender, &ts, &e.MsgType, &e.Body, &e.FormattedBody, &enc, &e.RawJSON); err != nil {
			return err
		}
		e.OriginServerTS, e.WasEncrypted = fromUnix(ts), enc != 0
		out.Edits = append(out.Edits, e)
		return nil
	}); err != nil {
		return fmt.Errorf("export edits: %w", err)
	}
	if err := scanAll(ctx, s.db, `
		select room_id, event_id, target_event_id, sender, key, origin_server_ts, was_encrypted
		from reactions order by origin_server_ts, event_id`, func(rows *sql.Rows) error {
		var r Reaction
		var ts int64
		var enc int
		if err := rows.Scan(&r.RoomID, &r.EventID, &r.TargetEventID, &r.Sender, &r.Key, &ts, &enc); err != nil {
			return err
		}
		r.OriginServerTS, r.WasEncrypted = fromUnix(ts), enc != 0
		out.Reactions = append(out.Reactions, r)
		return nil
	}); err != nil {
		return fmt.Errorf("export reactions: %w", err)
	}
	if err := scanAll(ctx, s.db, `
		select room_id, event_id, target_event_id, sender, origin_server_ts, coalesce(reason,'')
		from redactions order by origin_server_ts, event_id`, func(rows *sql.Rows) error {
		var r Redaction
		var ts int64
		if err := rows.Scan(&r.RoomID, &r.EventID, &r.TargetEventID, &r.Sender, &ts, &r.Reason); err != nil {
			return err
		}
		r.OriginServerTS = fromUnix(ts)
		out.Redactions = append(out.Redactions, r)
		return nil
	}); err != nil {
		return fmt.Errorf("export redactions: %w", err)
	}
	if err := scanAll(ctx, s.db, `
		select space_id, child_id, suggested, coalesce(order_key,''), coalesce(via_json,'')
		from space_children order by space_id, child_id`, func(rows *sql.Rows) error {
		var c SpaceChild
		var suggested int
		var via string
		if err := rows.Scan(&c.SpaceID, &c.ChildID, &suggested, &c.Order, &via); err != nil {
			return err
		}
		c.Suggested = suggested != 0
		if via != "" {
			_ = json.Unmarshal([]byte(via), &c.Via)
		}
		out.SpaceChildren = append(out.SpaceChildren, c)
		return nil
	}); err != nil {
		return fmt.Errorf("export space children: %w", err)
	}
	return nil
}

func scanAll(ctx context.Context, db *sql.DB, query string, fn func(*sql.Rows) error) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (snap Snapshot) Validate() error {
	if snap.SchemaVersion == 0 {
		return errors.New("snapshot missing schema_version")
	}
	for _, m := range snap.Messages {
		if m.RoomID == "" || m.EventID == "" {
			return fmt.Errorf("message missing room_id/event_id: %+v", m)
		}
	}
	return nil
}

func (s *Store) ImportSnapshot(ctx context.Context, snap Snapshot, source string, exported time.Time) error {
	stats := SyncStats{
		DBPath:     s.path,
		Rooms:      len(snap.Rooms),
		Messages:   len(snap.Messages),
		StartedAt:  exported,
		FinishedAt: time.Now().UTC(),
	}
	return s.Apply(ctx, stats, Batch{
		Rooms:         snap.Rooms,
		Members:       snap.Members,
		Messages:      snap.Messages,
		Edits:         snap.Edits,
		Reactions:     snap.Reactions,
		Redactions:    snap.Redactions,
		SpaceChildren: snap.SpaceChildren,
	})
}
