package store

const schemaSQL = `
create table if not exists rooms (
	id text primary key,
	canonical_alias text,
	name text,
	topic text,
	avatar_mxc text,
	is_direct integer not null default 0,
	is_encrypted integer not null default 0,
	encryption_algorithm text,
	member_count integer not null default 0,
	joined_at integer,
	last_event_ts integer,
	message_count integer not null default 0,
	prev_batch text
);

create table if not exists room_members (
	room_id text not null,
	user_id text not null,
	display_name text,
	avatar_mxc text,
	membership text not null,
	power_level integer not null default 0,
	primary key (room_id, user_id)
);

create table if not exists messages (
	rowid integer primary key autoincrement,
	room_id text not null,
	event_id text not null,
	sender text not null,
	sender_display_name text,
	origin_server_ts integer not null,
	msgtype text not null,
	body text,
	formatted_body text,
	was_encrypted integer not null default 0,
	decrypt_status text,
	decrypt_error text,
	attachments_json text,
	edits_json text,
	reactions_json text,
	thread_id text,
	reply_to_event_id text,
	raw_json text,
	unique(room_id, event_id)
);

create index if not exists idx_room_members_user on room_members(user_id);

create index if not exists idx_messages_room_ts on messages(room_id, origin_server_ts);
create index if not exists idx_messages_ts on messages(origin_server_ts);
create index if not exists idx_messages_sender on messages(sender);

create virtual table if not exists messages_fts using fts5(body, room, sender);

-- Relations are stored by their own event ID and point at their target, so
-- they land correctly whichever order history arrives in (backfill walks
-- backwards, so an edit or reaction is usually seen before its target).
create table if not exists edits (
	room_id text not null,
	event_id text not null,
	target_event_id text not null,
	sender text not null,
	origin_server_ts integer not null,
	msgtype text,
	body text,
	formatted_body text,
	was_encrypted integer not null default 0,
	raw_json text,
	primary key (room_id, event_id)
);
create index if not exists idx_edits_target on edits(room_id, target_event_id, origin_server_ts);

create table if not exists reactions (
	room_id text not null,
	event_id text not null,
	target_event_id text not null,
	sender text not null,
	key text not null,
	origin_server_ts integer not null,
	was_encrypted integer not null default 0,
	primary key (room_id, event_id)
);
create index if not exists idx_reactions_target on reactions(room_id, target_event_id);

create table if not exists redactions (
	room_id text not null,
	event_id text not null,
	target_event_id text not null,
	sender text not null,
	origin_server_ts integer not null,
	reason text,
	primary key (room_id, event_id)
);
create index if not exists idx_redactions_target on redactions(room_id, target_event_id);

create table if not exists space_children (
	space_id text not null,
	child_id text not null,
	suggested integer not null default 0,
	order_key text,
	via_json text,
	primary key (space_id, child_id)
);
create index if not exists idx_space_children_child on space_children(child_id);

create table if not exists sync_state (
	key text primary key,
	value text not null,
	updated_at integer not null
);
`

// addedColumns are columns introduced after a table was first created.
// `create table if not exists` won't add them to an existing archive, so
// Open adds whichever are missing.
var addedColumns = []struct{ table, column, decl string }{
	{"rooms", "room_type", "text"},
	{"rooms", "room_version", "text"},
	{"rooms", "creator", "text"},
	{"rooms", "created_at", "integer"},
	{"rooms", "predecessor_room_id", "text"},
	{"rooms", "replacement_room_id", "text"},
	{"rooms", "power_levels_json", "text"},
}
