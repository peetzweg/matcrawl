package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

type Store struct {
	db   *sql.DB
	path string
}

type SyncStats struct {
	DBPath     string    `json:"db_path"`
	Rooms      int       `json:"rooms"`
	Messages   int       `json:"messages"`
	NextBatch  string    `json:"next_batch,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

type Status struct {
	DBPath          string    `json:"db_path"`
	Rooms           int       `json:"rooms"`
	EncryptedRooms  int       `json:"encrypted_rooms"`
	Messages        int       `json:"messages"`
	DecryptFailures int       `json:"decrypt_failures"`
	MediaMessages   int       `json:"media_messages"`
	OldestEvent     time.Time `json:"oldest_event,omitzero"`
	NewestEvent     time.Time `json:"newest_event,omitzero"`
	LastSyncAt      time.Time `json:"last_sync_at,omitzero"`
	NextBatch       string    `json:"next_batch,omitempty"`
}

type Room struct {
	ID                  string    `json:"id"`
	CanonicalAlias      string    `json:"canonical_alias,omitempty"`
	Name                string    `json:"name,omitempty"`
	Topic               string    `json:"topic,omitempty"`
	AvatarMXC           string    `json:"avatar_mxc,omitempty"`
	IsDirect            bool      `json:"is_direct,omitempty"`
	IsEncrypted         bool      `json:"is_encrypted,omitempty"`
	EncryptionAlgorithm string    `json:"encryption_algorithm,omitempty"`
	MemberCount         int       `json:"member_count"`
	JoinedAt            time.Time `json:"joined_at,omitzero"`
	LastEventTS         time.Time `json:"last_event_ts,omitzero"`
	MessageCount        int       `json:"message_count"`
	PrevBatch           string    `json:"prev_batch,omitempty"`
}

type RoomMember struct {
	RoomID      string `json:"room_id"`
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarMXC   string `json:"avatar_mxc,omitempty"`
	Membership  string `json:"membership"`
	PowerLevel  int    `json:"power_level"`
}

type Message struct {
	RoomID            string    `json:"room_id"`
	EventID           string    `json:"event_id"`
	Sender            string    `json:"sender"`
	SenderDisplayName string    `json:"sender_display_name,omitempty"`
	OriginServerTS    time.Time `json:"origin_server_ts"`
	MsgType           string    `json:"msgtype"`
	Body              string    `json:"body,omitempty"`
	FormattedBody     string    `json:"formatted_body,omitempty"`
	WasEncrypted      bool      `json:"was_encrypted,omitempty"`
	DecryptStatus     string    `json:"decrypt_status,omitempty"`
	DecryptError      string    `json:"decrypt_error,omitempty"`
	AttachmentsJSON   string    `json:"attachments_json,omitempty"`
	EditsJSON         string    `json:"edits_json,omitempty"`
	ReactionsJSON     string    `json:"reactions_json,omitempty"`
	ThreadID          string    `json:"thread_id,omitempty"`
	ReplyToEventID    string    `json:"reply_to_event_id,omitempty"`
	RawJSON           string    `json:"raw_json,omitempty"`
	Snippet           string    `json:"snippet,omitempty"`
}

type MessageFilter struct {
	Query    string
	RoomID   string
	Sender   string
	Limit    int
	After    *time.Time
	Before   *time.Time
	HasMedia bool
	Asc      bool
}

type RoomFilter struct {
	Limit         int
	EncryptedOnly bool
}

func Open(ctx context.Context, path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("db path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir db dir: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("pragma user_version = %d", schemaVersion)); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }
func (s *Store) DB() *sql.DB  { return s.db }

// Upsert merges a batch of rooms, members, and messages into the store.
// Safe to call repeatedly with overlapping inputs; FTS rows are rebuilt per
// message so re-decryption (e.g. after `matcrawl keys retry`) replaces the
// indexed body in-place.
func (s *Store) Upsert(ctx context.Context, stats SyncStats, rooms []Room, members []RoomMember, messages []Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, r := range rooms {
		if _, err := tx.ExecContext(ctx, `
			insert into rooms(id,canonical_alias,name,topic,avatar_mxc,is_direct,is_encrypted,encryption_algorithm,member_count,joined_at,last_event_ts,message_count,prev_batch)
			values(?,?,?,?,?,?,?,?,?,?,?,?,?)
			on conflict(id) do update set
				canonical_alias=excluded.canonical_alias,
				name=excluded.name,
				topic=excluded.topic,
				avatar_mxc=excluded.avatar_mxc,
				is_direct=excluded.is_direct,
				is_encrypted=excluded.is_encrypted,
				encryption_algorithm=excluded.encryption_algorithm,
				member_count=excluded.member_count,
				joined_at=case when excluded.joined_at > 0 then excluded.joined_at else rooms.joined_at end,
				last_event_ts=case when excluded.last_event_ts > coalesce(rooms.last_event_ts,0) then excluded.last_event_ts else rooms.last_event_ts end,
				message_count=excluded.message_count,
				prev_batch=case when excluded.prev_batch <> '' then excluded.prev_batch else rooms.prev_batch end
		`, r.ID, r.CanonicalAlias, r.Name, r.Topic, r.AvatarMXC, boolInt(r.IsDirect), boolInt(r.IsEncrypted), r.EncryptionAlgorithm, r.MemberCount, unix(r.JoinedAt), unix(r.LastEventTS), r.MessageCount, r.PrevBatch); err != nil {
			return fmt.Errorf("upsert room %s: %w", r.ID, err)
		}
	}

	for _, mem := range members {
		if _, err := tx.ExecContext(ctx, `
			insert into room_members(room_id,user_id,display_name,avatar_mxc,membership,power_level)
			values(?,?,?,?,?,?)
			on conflict(room_id,user_id) do update set
				display_name=excluded.display_name,
				avatar_mxc=excluded.avatar_mxc,
				membership=excluded.membership,
				power_level=excluded.power_level
		`, mem.RoomID, mem.UserID, mem.DisplayName, mem.AvatarMXC, mem.Membership, mem.PowerLevel); err != nil {
			return fmt.Errorf("upsert member %s/%s: %w", mem.RoomID, mem.UserID, err)
		}
	}

	for _, m := range messages {
		if _, err := tx.ExecContext(ctx, `
			insert into messages(room_id,event_id,sender,sender_display_name,origin_server_ts,msgtype,body,formatted_body,was_encrypted,decrypt_status,decrypt_error,attachments_json,edits_json,reactions_json,thread_id,reply_to_event_id,raw_json)
			values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			on conflict(room_id,event_id) do update set
				sender=excluded.sender,
				sender_display_name=excluded.sender_display_name,
				origin_server_ts=excluded.origin_server_ts,
				msgtype=excluded.msgtype,
				body=excluded.body,
				formatted_body=excluded.formatted_body,
				was_encrypted=excluded.was_encrypted,
				decrypt_status=excluded.decrypt_status,
				decrypt_error=excluded.decrypt_error,
				attachments_json=excluded.attachments_json,
				edits_json=excluded.edits_json,
				reactions_json=excluded.reactions_json,
				thread_id=excluded.thread_id,
				reply_to_event_id=excluded.reply_to_event_id,
				raw_json=excluded.raw_json
		`, m.RoomID, m.EventID, m.Sender, m.SenderDisplayName, unix(m.OriginServerTS), m.MsgType, m.Body, m.FormattedBody, boolInt(m.WasEncrypted), m.DecryptStatus, m.DecryptError, m.AttachmentsJSON, m.EditsJSON, m.ReactionsJSON, m.ThreadID, m.ReplyToEventID, m.RawJSON); err != nil {
			return fmt.Errorf("upsert message %s/%s: %w", m.RoomID, m.EventID, err)
		}

		// FTS5 has no ON CONFLICT support, so rebuild the row by hand.
		if _, err := tx.ExecContext(ctx,
			`delete from messages_fts where rowid=(select rowid from messages where room_id=? and event_id=?)`,
			m.RoomID, m.EventID); err != nil {
			return fmt.Errorf("fts delete: %w", err)
		}
		if ftsIndexable(m) {
			roomLabel := m.RoomID
			if _, err := tx.ExecContext(ctx,
				`insert into messages_fts(rowid,body,room,sender) values((select rowid from messages where room_id=? and event_id=?),?,?,?)`,
				m.RoomID, m.EventID, strings.TrimSpace(m.Body), roomLabel, displayOrID(m.SenderDisplayName, m.Sender)); err != nil {
				return fmt.Errorf("fts insert: %w", err)
			}
		}
	}

	now := stats.FinishedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	updates := map[string]string{
		"last_sync_at": now.Format(time.RFC3339Nano),
	}
	if stats.NextBatch != "" {
		updates["next_batch"] = stats.NextBatch
	}
	for key, value := range updates {
		if _, err := tx.ExecContext(ctx, `
			insert into sync_state(key,value,updated_at) values(?,?,?)
			on conflict(key) do update set value=excluded.value, updated_at=excluded.updated_at
		`, key, value, unix(now)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SetSyncState upserts a single sync_state row; used by sync/backfill loops
// to checkpoint tokens between batches without going through Upsert.
func (s *Store) SetSyncState(ctx context.Context, key, value string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		insert into sync_state(key,value,updated_at) values(?,?,?)
		on conflict(key) do update set value=excluded.value, updated_at=excluded.updated_at
	`, key, value, unix(now))
	return err
}

func (s *Store) GetSyncState(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `select value from sync_state where key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	out := Status{DBPath: s.path}
	for _, c := range []struct {
		dst *int
		q   string
	}{
		{&out.Rooms, "select count(*) from rooms"},
		{&out.EncryptedRooms, "select count(*) from rooms where is_encrypted <> 0"},
		{&out.Messages, "select count(*) from messages"},
		{&out.DecryptFailures, "select count(*) from messages where decrypt_status in ('missing_keys','failed')"},
		{&out.MediaMessages, "select count(*) from messages where msgtype in ('m.image','m.video','m.audio','m.file')"},
	} {
		if err := s.db.QueryRowContext(ctx, c.q).Scan(c.dst); err != nil {
			return out, err
		}
	}
	var oldest, newest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `select min(origin_server_ts), max(origin_server_ts) from messages`).Scan(&oldest, &newest); err != nil {
		return out, err
	}
	if oldest.Valid {
		out.OldestEvent = fromUnix(oldest.Int64)
	}
	if newest.Valid {
		out.NewestEvent = fromUnix(newest.Int64)
	}
	var lastSync string
	_ = s.db.QueryRowContext(ctx, `select value from sync_state where key='last_sync_at'`).Scan(&lastSync)
	if t, err := time.Parse(time.RFC3339Nano, lastSync); err == nil {
		out.LastSyncAt = t
	}
	_ = s.db.QueryRowContext(ctx, `select value from sync_state where key='next_batch'`).Scan(&out.NextBatch)
	return out, nil
}

func (s *Store) ListRooms(ctx context.Context, filter RoomFilter) ([]Room, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	query := `
		select id, coalesce(canonical_alias,''), coalesce(name,''), coalesce(topic,''), coalesce(avatar_mxc,''),
		       is_direct, is_encrypted, coalesce(encryption_algorithm,''), member_count,
		       coalesce(joined_at,0), coalesce(last_event_ts,0), message_count, coalesce(prev_batch,'')
		from rooms
	`
	args := []any{}
	if filter.EncryptedOnly {
		query += " where is_encrypted <> 0"
	}
	query += " order by last_event_ts desc nulls last limit ?"
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Room
	for rows.Next() {
		var r Room
		var isDirect, isEncrypted int
		var joinedAt, lastEventTS int64
		if err := rows.Scan(&r.ID, &r.CanonicalAlias, &r.Name, &r.Topic, &r.AvatarMXC,
			&isDirect, &isEncrypted, &r.EncryptionAlgorithm, &r.MemberCount,
			&joinedAt, &lastEventTS, &r.MessageCount, &r.PrevBatch); err != nil {
			return nil, err
		}
		r.IsDirect = isDirect != 0
		r.IsEncrypted = isEncrypted != 0
		r.JoinedAt = fromUnix(joinedAt)
		r.LastEventTS = fromUnix(lastEventTS)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListUndecrypted returns messages that arrived as m.room.encrypted but
// haven't been decrypted yet (decrypt_status = 'missing_keys' or 'failed').
// Selects raw_json so callers can re-run them through OlmMachine after a
// fresh key import.
func (s *Store) ListUndecrypted(ctx context.Context, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
		select room_id, event_id, sender, coalesce(sender_display_name,''),
		       origin_server_ts, msgtype, coalesce(body,''), coalesce(formatted_body,''),
		       was_encrypted, coalesce(decrypt_status,''), coalesce(decrypt_error,''),
		       coalesce(attachments_json,''), coalesce(edits_json,''), coalesce(reactions_json,''),
		       coalesce(thread_id,''), coalesce(reply_to_event_id,''), coalesce(raw_json,'')
		from messages
		where decrypt_status in ('missing_keys','failed') and coalesce(raw_json,'') <> ''
		order by origin_server_ts asc
		limit ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Message
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
			return nil, err
		}
		m.OriginServerTS = fromUnix(ts)
		m.WasEncrypted = wasEnc != 0
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) ListMembers(ctx context.Context, roomID string) ([]RoomMember, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, errors.New("room id is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		select room_id, user_id, coalesce(display_name,''), coalesce(avatar_mxc,''), membership, power_level
		from room_members
		where room_id = ?
		order by power_level desc, user_id asc
	`, roomID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []RoomMember
	for rows.Next() {
		var m RoomMember
		if err := rows.Scan(&m.RoomID, &m.UserID, &m.DisplayName, &m.AvatarMXC, &m.Membership, &m.PowerLevel); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) Messages(ctx context.Context, filter MessageFilter) ([]Message, error) {
	return s.messages(ctx, filter, false)
}

func (s *Store) Search(ctx context.Context, filter MessageFilter) ([]Message, error) {
	if strings.TrimSpace(filter.Query) == "" {
		return nil, errors.New("search query required")
	}
	return s.messages(ctx, filter, true)
}

func (s *Store) messages(ctx context.Context, filter MessageFilter, search bool) ([]Message, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	var query string
	args := []any{}
	prefix := ""
	if search {
		query = `
			select m.room_id, m.event_id, m.sender, coalesce(m.sender_display_name,''),
			       m.origin_server_ts, m.msgtype, coalesce(m.body,''), coalesce(m.formatted_body,''),
			       m.was_encrypted, coalesce(m.decrypt_status,''), coalesce(m.decrypt_error,''),
			       coalesce(m.attachments_json,''), coalesce(m.edits_json,''), coalesce(m.reactions_json,''),
			       coalesce(m.thread_id,''), coalesce(m.reply_to_event_id,''),
			       snippet(messages_fts,0,'[',']','…',12) as snippet
			from messages_fts f
			join messages m on m.rowid = f.rowid
			where messages_fts match ?
		`
		args = append(args, filter.Query)
		prefix = "m."
	} else {
		query = `
			select room_id, event_id, sender, coalesce(sender_display_name,''),
			       origin_server_ts, msgtype, coalesce(body,''), coalesce(formatted_body,''),
			       was_encrypted, coalesce(decrypt_status,''), coalesce(decrypt_error,''),
			       coalesce(attachments_json,''), coalesce(edits_json,''), coalesce(reactions_json,''),
			       coalesce(thread_id,''), coalesce(reply_to_event_id,''),
			       '' as snippet
			from messages where 1=1
		`
	}
	if filter.RoomID != "" {
		query += " and " + prefix + "room_id = ?"
		args = append(args, filter.RoomID)
	}
	if filter.Sender != "" {
		query += " and " + prefix + "sender = ?"
		args = append(args, filter.Sender)
	}
	if filter.After != nil {
		query += " and " + prefix + "origin_server_ts >= ?"
		args = append(args, unix(*filter.After))
	}
	if filter.Before != nil {
		query += " and " + prefix + "origin_server_ts <= ?"
		args = append(args, unix(*filter.Before))
	}
	if filter.HasMedia {
		query += " and " + prefix + "msgtype in ('m.image','m.video','m.audio','m.file')"
	}
	switch {
	case search:
		query += " order by bm25(messages_fts) limit ?"
	case filter.Asc:
		query += " order by " + prefix + "origin_server_ts asc limit ?"
	default:
		query += " order by " + prefix + "origin_server_ts desc limit ?"
	}
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Message
	for rows.Next() {
		var m Message
		var ts int64
		var wasEnc int
		if err := rows.Scan(
			&m.RoomID, &m.EventID, &m.Sender, &m.SenderDisplayName,
			&ts, &m.MsgType, &m.Body, &m.FormattedBody,
			&wasEnc, &m.DecryptStatus, &m.DecryptError,
			&m.AttachmentsJSON, &m.EditsJSON, &m.ReactionsJSON,
			&m.ThreadID, &m.ReplyToEventID, &m.Snippet,
		); err != nil {
			return nil, err
		}
		m.OriginServerTS = fromUnix(ts)
		m.WasEncrypted = wasEnc != 0
		out = append(out, m)
	}
	return out, rows.Err()
}

// ftsIndexable reports whether a message's body should land in messages_fts.
// FTS-on-ciphertext is useless, so encrypted events that have not been
// decrypted ('missing_keys', 'failed') are skipped; plaintext and successfully
// decrypted events (” or 'ok') are indexed.
func ftsIndexable(m Message) bool {
	if strings.TrimSpace(m.Body) == "" {
		return false
	}
	switch m.DecryptStatus {
	case "", "ok":
		return true
	default:
		return false
	}
}

func displayOrID(display, id string) string {
	if strings.TrimSpace(display) != "" {
		return display
	}
	return id
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}

func unix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func fromUnix(s int64) time.Time {
	if s == 0 {
		return time.Time{}
	}
	return time.Unix(s, 0).UTC()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
