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

const schemaVersion = 2

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
	Edits           int       `json:"edits"`
	Reactions       int       `json:"reactions"`
	Redactions      int       `json:"redactions"`
	Spaces          int       `json:"spaces"`
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
	DisplayName         string    `json:"display_name,omitempty"`
	IsDirect            bool      `json:"is_direct,omitempty"`
	IsEncrypted         bool      `json:"is_encrypted,omitempty"`
	EncryptionAlgorithm string    `json:"encryption_algorithm,omitempty"`
	MemberCount         int       `json:"member_count"`
	JoinedAt            time.Time `json:"joined_at,omitzero"`
	LastEventTS         time.Time `json:"last_event_ts,omitzero"`
	MessageCount        int       `json:"message_count"`
	PrevBatch           string    `json:"prev_batch,omitempty"`
	// SeedPrevBatch is a backfill cursor offered by /sync: it starts a room's
	// backfill but never moves an existing cursor, which only backfill
	// advances. Not persisted on its own.
	SeedPrevBatch     string    `json:"-"`
	RoomType          string    `json:"room_type,omitempty"`
	RoomVersion       string    `json:"room_version,omitempty"`
	Creator           string    `json:"creator,omitempty"`
	CreatedAt         time.Time `json:"created_at,omitzero"`
	PredecessorRoomID string    `json:"predecessor_room_id,omitempty"`
	ReplacementRoomID string    `json:"replacement_room_id,omitempty"`
	PowerLevelsJSON   string    `json:"power_levels_json,omitempty"`
	ParentSpaces      []RoomRef `json:"parent_spaces,omitempty"`
}

// RoomRef names a room by ID and its display name.
type RoomRef struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
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
	RoomName          string    `json:"room_name,omitempty"`
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
	ThreadID          string    `json:"thread_id,omitempty"`
	ReplyToEventID    string    `json:"reply_to_event_id,omitempty"`
	RawJSON           string    `json:"raw_json,omitempty"`
	Snippet           string    `json:"snippet,omitempty"`

	// Read-time view of the message's relations; never written back.
	EditedAt     time.Time       `json:"edited_at,omitzero"`
	OriginalBody string          `json:"original_body,omitempty"`
	Edits        []EditRevision  `json:"edits,omitempty"`
	Reactions    []ReactionCount `json:"reactions,omitempty"`
	Redacted     *RedactionInfo  `json:"redacted,omitempty"`
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
	// SpaceID restricts to rooms that space lists as children.
	SpaceID string
}

type RoomFilter struct {
	// Limit caps the result set. Zero or negative means "no limit".
	Limit int
	// EncryptedOnly restricts to rooms with E2EE enabled.
	EncryptedOnly bool
	// Search is a case-insensitive substring match across id, name, topic,
	// canonical_alias. Empty matches everything.
	Search string
	// SpaceID restricts to rooms that space lists as children.
	SpaceID string
	// SpacesOnly restricts to rooms that are themselves spaces.
	SpacesOnly bool
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
	if err := addMissingColumns(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("pragma user_version = %d", schemaVersion)); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func addMissingColumns(ctx context.Context, db *sql.DB) error {
	for _, c := range addedColumns {
		var n int
		if err := db.QueryRowContext(ctx, `select count(*) from pragma_table_info(?) where name = ?`, c.table, c.column).Scan(&n); err != nil {
			return fmt.Errorf("inspect %s.%s: %w", c.table, c.column, err)
		}
		if n > 0 {
			continue
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("alter table %s add column %s %s", c.table, c.column, c.decl)); err != nil {
			return fmt.Errorf("add %s.%s: %w", c.table, c.column, err)
		}
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Path() string { return s.path }
func (s *Store) DB() *sql.DB  { return s.db }

// Upsert merges a batch of rooms, members, and messages into the store.
// Safe to call repeatedly with overlapping inputs; FTS rows are rebuilt per
// message so re-decryption (e.g. after `matcrawl keys retry`) replaces the
// indexed body in-place.
func (s *Store) Upsert(ctx context.Context, stats SyncStats, rooms []Room, members []RoomMember, messages []Message) error {
	return s.Apply(ctx, stats, Batch{Rooms: rooms, Members: members, Messages: messages})
}

// Apply writes everything in b in one transaction, so a pagination
// checkpoint can never get ahead of the events it covers.
func (s *Store) Apply(ctx context.Context, stats SyncStats, b Batch) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	// An incremental /sync (and every backfill page) only carries state that
	// changed, so an empty field means "unknown", not "cleared": keep what we
	// already have. is_direct is owned by SetDirectRooms (m.direct), and
	// encryption can never be turned off in Matrix.
	for _, r := range b.Rooms {
		if _, err := tx.ExecContext(ctx, `
			insert into rooms(id,canonical_alias,name,topic,avatar_mxc,is_direct,is_encrypted,encryption_algorithm,member_count,joined_at,last_event_ts,message_count,prev_batch,
			                  room_type,room_version,creator,created_at,predecessor_room_id,replacement_room_id,power_levels_json)
			values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			on conflict(id) do update set
				canonical_alias=coalesce(nullif(excluded.canonical_alias,''), rooms.canonical_alias),
				name=coalesce(nullif(excluded.name,''), rooms.name),
				topic=coalesce(nullif(excluded.topic,''), rooms.topic),
				avatar_mxc=coalesce(nullif(excluded.avatar_mxc,''), rooms.avatar_mxc),
				is_direct=rooms.is_direct,
				is_encrypted=max(excluded.is_encrypted, rooms.is_encrypted),
				encryption_algorithm=coalesce(nullif(excluded.encryption_algorithm,''), rooms.encryption_algorithm),
				member_count=case when excluded.member_count > 0 then excluded.member_count else rooms.member_count end,
				joined_at=case when excluded.joined_at > 0 then excluded.joined_at else rooms.joined_at end,
				last_event_ts=case when excluded.last_event_ts > coalesce(rooms.last_event_ts,0) then excluded.last_event_ts else rooms.last_event_ts end,
				message_count=excluded.message_count,
				prev_batch=case
					when ? <> '' then excluded.prev_batch
					when coalesce(rooms.prev_batch,'') = '' then excluded.prev_batch
					else rooms.prev_batch end,
				room_type=coalesce(nullif(excluded.room_type,''), rooms.room_type),
				room_version=coalesce(nullif(excluded.room_version,''), rooms.room_version),
				creator=coalesce(nullif(excluded.creator,''), rooms.creator),
				created_at=case when excluded.created_at > 0 then excluded.created_at else rooms.created_at end,
				predecessor_room_id=coalesce(nullif(excluded.predecessor_room_id,''), rooms.predecessor_room_id),
				replacement_room_id=coalesce(nullif(excluded.replacement_room_id,''), rooms.replacement_room_id),
				power_levels_json=coalesce(nullif(excluded.power_levels_json,''), rooms.power_levels_json)
		`, r.ID, r.CanonicalAlias, r.Name, r.Topic, r.AvatarMXC, boolInt(r.IsDirect), boolInt(r.IsEncrypted), r.EncryptionAlgorithm, r.MemberCount, unix(r.JoinedAt), unix(r.LastEventTS), r.MessageCount, firstNonEmpty(r.PrevBatch, r.SeedPrevBatch),
			r.RoomType, r.RoomVersion, r.Creator, unix(r.CreatedAt), r.PredecessorRoomID, r.ReplacementRoomID, r.PowerLevelsJSON,
			r.PrevBatch); err != nil {
			return fmt.Errorf("upsert room %s: %w", r.ID, err)
		}
	}

	for _, mem := range b.Members {
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

	reindex := map[EventRef]struct{}{}
	touched := map[string]struct{}{}
	for _, r := range b.Rooms {
		touched[r.ID] = struct{}{}
	}

	for _, m := range b.Messages {
		if _, err := tx.ExecContext(ctx, `
			insert into messages(room_id,event_id,sender,sender_display_name,origin_server_ts,msgtype,body,formatted_body,was_encrypted,decrypt_status,decrypt_error,attachments_json,thread_id,reply_to_event_id,raw_json)
			values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
				thread_id=excluded.thread_id,
				reply_to_event_id=excluded.reply_to_event_id,
				raw_json=excluded.raw_json
		`, m.RoomID, m.EventID, m.Sender, m.SenderDisplayName, unix(m.OriginServerTS), m.MsgType, m.Body, m.FormattedBody, boolInt(m.WasEncrypted), m.DecryptStatus, m.DecryptError, m.AttachmentsJSON, m.ThreadID, m.ReplyToEventID, m.RawJSON); err != nil {
			return fmt.Errorf("upsert message %s/%s: %w", m.RoomID, m.EventID, err)
		}
		reindex[EventRef{m.RoomID, m.EventID}] = struct{}{}
		touched[m.RoomID] = struct{}{}
	}

	if err := applyRelations(ctx, tx, b, reindex, touched); err != nil {
		return err
	}
	for ref := range reindex {
		if err := reindexFTS(ctx, tx, ref); err != nil {
			return err
		}
	}

	// Refresh denormalized rollups on every room we just touched. Cheap with
	// the (room_id, origin_server_ts) index. Without this `rooms.message_count`
	// and `rooms.last_event_ts` would stay stale at 0 / first-seen.
	for roomID := range touched {
		if _, err := tx.ExecContext(ctx, `
			update rooms set
				message_count = (select count(*) from messages where room_id = ?),
				last_event_ts = coalesce(
					(select max(origin_server_ts) from messages where room_id = ?),
					last_event_ts
				)
			where id = ?
		`, roomID, roomID, roomID); err != nil {
			return fmt.Errorf("refresh rollups for %s: %w", roomID, err)
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
	for key, value := range b.SyncState {
		updates[key] = value
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
		{&out.Edits, "select count(*) from edits"},
		{&out.Reactions, "select count(*) from reactions"},
		{&out.Redactions, "select count(*) from redactions"},
		{&out.Spaces, "select count(*) from rooms where room_type = 'm.space'"},
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
	query := `
		select id, coalesce(canonical_alias,''), coalesce(name,''), coalesce(topic,''), coalesce(avatar_mxc,''),
		       is_direct, is_encrypted, coalesce(encryption_algorithm,''), member_count,
		       coalesce(joined_at,0), coalesce(last_event_ts,0), message_count, coalesce(prev_batch,''),
		       coalesce(room_type,''), coalesce(room_version,''), coalesce(creator,''), coalesce(created_at,0),
		       coalesce(predecessor_room_id,''), coalesce(replacement_room_id,''), coalesce(power_levels_json,'')
		from rooms
		where 1=1
	`
	args := []any{}
	if filter.SpaceID != "" {
		query += " and id in (select child_id from space_children where space_id = ?)"
		args = append(args, filter.SpaceID)
	}
	if filter.SpacesOnly {
		query += " and room_type = 'm.space'"
	}
	if filter.EncryptedOnly {
		query += " and is_encrypted <> 0"
	}
	if term := strings.TrimSpace(filter.Search); term != "" {
		query += ` and (
			lower(coalesce(name,''))            like lower(?) or
			lower(coalesce(canonical_alias,'')) like lower(?) or
			lower(coalesce(topic,''))           like lower(?) or
			lower(id)                           like lower(?)
		)`
		like := "%" + term + "%"
		args = append(args, like, like, like, like)
	}
	query += " order by last_event_ts desc nulls last, name asc"
	if filter.Limit > 0 {
		query += " limit ?"
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Room
	for rows.Next() {
		var r Room
		var isDirect, isEncrypted int
		var joinedAt, lastEventTS, createdAt int64
		if err := rows.Scan(&r.ID, &r.CanonicalAlias, &r.Name, &r.Topic, &r.AvatarMXC,
			&isDirect, &isEncrypted, &r.EncryptionAlgorithm, &r.MemberCount,
			&joinedAt, &lastEventTS, &r.MessageCount, &r.PrevBatch,
			&r.RoomType, &r.RoomVersion, &r.Creator, &createdAt,
			&r.PredecessorRoomID, &r.ReplacementRoomID, &r.PowerLevelsJSON); err != nil {
			return nil, err
		}
		r.CreatedAt = fromUnix(createdAt)
		r.IsDirect = isDirect != 0
		r.IsEncrypted = isEncrypted != 0
		r.JoinedAt = fromUnix(joinedAt)
		r.LastEventTS = fromUnix(lastEventTS)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	labels, err := s.RoomLabels(ctx)
	if err != nil {
		return nil, err
	}
	parents, err := s.parentSpaces(ctx, labels)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].DisplayName = labels[out[i].ID]
		out[i].ParentSpaces = parents[out[i].ID]
	}
	return out, nil
}

// parentSpaces maps each room to the spaces that list it as a child.
func (s *Store) parentSpaces(ctx context.Context, labels map[string]string) (map[string][]RoomRef, error) {
	rows, err := s.db.QueryContext(ctx, `select child_id, space_id from space_children order by space_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]RoomRef{}
	for rows.Next() {
		var child, space string
		if err := rows.Scan(&child, &space); err != nil {
			return nil, err
		}
		out[child] = append(out[child], RoomRef{ID: space, Name: labels[space]})
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
		       coalesce(attachments_json,''),
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
			&m.AttachmentsJSON,
			&m.ThreadID, &m.ReplyToEventID, &m.RawJSON,
		); err != nil {
			return nil, err
		}
		m.OriginServerTS = fromUnix(ts)
		m.WasEncrypted = wasEnc != 0
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	labels, err := s.RoomLabels(ctx)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].RoomName = labels[out[i].RoomID]
	}
	return out, nil
}

// powerLevelSQL reads a member's power level from the room's
// m.room.power_levels content: their entry in users, else users_default.
// Rooms with no recorded power levels fall back to the stored column.
const powerLevelSQL = `cast(case when coalesce(r.power_levels_json,'') = '' then m.power_level else coalesce(
	json_extract(r.power_levels_json, '$.users."' || m.user_id || '"'),
	json_extract(r.power_levels_json, '$.users_default'),
	0) end as integer)`

func (s *Store) ListMembers(ctx context.Context, roomID string) ([]RoomMember, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, errors.New("room id is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		select m.room_id, m.user_id, coalesce(m.display_name,''), coalesce(m.avatar_mxc,''), m.membership,
		       `+powerLevelSQL+` as pl
		from room_members m
		left join rooms r on r.id = m.room_id
		where m.room_id = ?
		order by pl desc, m.user_id asc
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

// senderNameSQL resolves a message's sender display name at read time: the
// name stored on the event (if any), else the sender's current name in that
// room, else their name in any other room we share. Resolving on read keeps
// renames current without rewriting history.
const senderNameSQL = `coalesce(
	nullif(m.sender_display_name,''),
	(select nullif(rm.display_name,'') from room_members rm where rm.room_id = m.room_id and rm.user_id = m.sender),
	(select rm.display_name from room_members rm where rm.user_id = m.sender and coalesce(rm.display_name,'') <> '' limit 1),
	'')`

func (s *Store) messages(ctx context.Context, filter MessageFilter, search bool) ([]Message, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	var query string
	args := []any{}
	prefix := ""
	if search {
		query = `
			select m.room_id, m.event_id, m.sender, ` + senderNameSQL + `,
			       m.origin_server_ts, m.msgtype, coalesce(m.body,''), coalesce(m.formatted_body,''),
			       m.was_encrypted, coalesce(m.decrypt_status,''), coalesce(m.decrypt_error,''),
			       coalesce(m.attachments_json,''),
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
			select m.room_id, m.event_id, m.sender, ` + senderNameSQL + `,
			       m.origin_server_ts, m.msgtype, coalesce(m.body,''), coalesce(m.formatted_body,''),
			       m.was_encrypted, coalesce(m.decrypt_status,''), coalesce(m.decrypt_error,''),
			       coalesce(m.attachments_json,''),
			       coalesce(m.thread_id,''), coalesce(m.reply_to_event_id,''),
			       '' as snippet
			from messages m where 1=1
		`
		prefix = "m."
	}
	if filter.RoomID != "" {
		query += " and " + prefix + "room_id = ?"
		args = append(args, filter.RoomID)
	}
	if filter.SpaceID != "" {
		query += " and " + prefix + "room_id in (select child_id from space_children where space_id = ?)"
		args = append(args, filter.SpaceID)
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
			&m.AttachmentsJSON,
			&m.ThreadID, &m.ReplyToEventID, &m.Snippet,
		); err != nil {
			return nil, err
		}
		m.OriginServerTS = fromUnix(ts)
		m.WasEncrypted = wasEnc != 0
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	labels, err := s.RoomLabels(ctx)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].RoomName = labels[out[i].RoomID]
	}
	if err := s.enrichMessages(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
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

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
