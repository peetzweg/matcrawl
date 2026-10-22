package store

import (
	"context"
	"fmt"
	"strings"
)

// maxHeroes mirrors the Matrix spec's room-summary heroes: at most five other
// members name an unnamed room.
const maxHeroes = 5

// RoomLabels returns a human-readable name for every room, following the
// Matrix spec's display-name calculation (Client-Server API §13.1.1):
// m.room.name, then m.room.canonical_alias, then the other members' display
// names ("Alice and Bob", "Alice, Bob and 3 others"), then "Empty room (was
// Alice)". Rooms with nothing to go on are omitted; callers fall back to the
// room ID.
//
// The archive owner is excluded from member-derived names via the user_id
// sync_state key, which sync records on every run.
func (s *Store) RoomLabels(ctx context.Context) (map[string]string, error) {
	self, err := s.GetSyncState(ctx, "user_id")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	rows, err := s.db.QueryContext(ctx, `select id, coalesce(name,''), coalesce(canonical_alias,'') from rooms`)
	if err != nil {
		return nil, err
	}
	var unnamed []string
	for rows.Next() {
		var roomID, name, alias string
		if err := rows.Scan(&roomID, &name, &alias); err != nil {
			_ = rows.Close()
			return nil, err
		}
		switch {
		case strings.TrimSpace(name) != "":
			out[roomID] = name
		case alias != "":
			out[roomID] = alias
		default:
			unnamed = append(unnamed, roomID)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(unnamed) == 0 {
		return out, nil
	}

	type roster struct {
		current []string // joined or invited
		former  []string // left, kicked, banned
	}
	rosters := make(map[string]*roster, len(unnamed))
	for _, id := range unnamed {
		rosters[id] = &roster{}
	}
	mrows, err := s.db.QueryContext(ctx, `
		select m.room_id, m.user_id, coalesce(m.display_name,''), m.membership
		from room_members m
		join rooms r on r.id = m.room_id
		where coalesce(r.name,'') = '' and coalesce(r.canonical_alias,'') = '' and m.user_id <> ?
		order by m.room_id, m.user_id
	`, self)
	if err != nil {
		return nil, err
	}
	defer func() { _ = mrows.Close() }()
	for mrows.Next() {
		var roomID, userID, display, membership string
		if err := mrows.Scan(&roomID, &userID, &display, &membership); err != nil {
			return nil, err
		}
		r := rosters[roomID]
		if r == nil {
			continue
		}
		if strings.TrimSpace(display) == "" {
			display = userID
		}
		switch membership {
		case "join", "invite":
			r.current = append(r.current, display)
		default:
			r.former = append(r.former, display)
		}
	}
	if err := mrows.Err(); err != nil {
		return nil, err
	}
	for roomID, r := range rosters {
		switch {
		case len(r.current) > 0:
			out[roomID] = heroName(r.current)
		case len(r.former) > 0:
			out[roomID] = "Empty room (was " + heroName(r.former) + ")"
		}
	}
	return out, nil
}

func heroName(names []string) string {
	switch n := len(names); {
	case n == 1:
		return names[0]
	case n <= maxHeroes:
		return strings.Join(names[:n-1], ", ") + " and " + names[n-1]
	case n == maxHeroes+1:
		return strings.Join(names[:maxHeroes], ", ") + " and 1 other"
	default:
		return fmt.Sprintf("%s and %d others", strings.Join(names[:maxHeroes], ", "), n-maxHeroes)
	}
}

// SetDirectRooms marks exactly the given rooms as direct chats, mirroring the
// account's m.direct account data. Rooms not listed are cleared.
func (s *Store) SetDirectRooms(ctx context.Context, roomIDs []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `update rooms set is_direct = 0 where is_direct <> 0`); err != nil {
		return err
	}
	for _, id := range roomIDs {
		if _, err := tx.ExecContext(ctx, `update rooms set is_direct = 1 where id = ?`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LeftRoomsMissingName returns rooms the archive owner has left that have
// neither a name nor a canonical alias. A full-state /sync omits left rooms,
// so their state has to be fetched one room at a time.
func (s *Store) LeftRoomsMissingName(ctx context.Context, self string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		select r.id from rooms r
		left join room_members m on m.room_id = r.id and m.user_id = ?
		where coalesce(r.name,'') = '' and coalesce(r.canonical_alias,'') = ''
		  and coalesce(m.membership,'') not in ('join','invite')
		order by r.id
	`, self)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
