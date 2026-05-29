package store

import (
	"context"
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
		       coalesce(attachments_json,''), coalesce(edits_json,''), coalesce(reactions_json,''),
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
			&m.AttachmentsJSON, &m.EditsJSON, &m.ReactionsJSON,
			&m.ThreadID, &m.ReplyToEventID, &m.RawJSON,
		); err != nil {
			return out, err
		}
		m.OriginServerTS = fromUnix(ts)
		m.WasEncrypted = wasEnc != 0
		out.Messages = append(out.Messages, m)
	}
	return out, rows.Err()
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
	return s.Upsert(ctx, stats, snap.Rooms, snap.Members, snap.Messages)
}
