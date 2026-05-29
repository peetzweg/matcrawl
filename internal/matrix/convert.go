package matrix

import (
	"encoding/json"
	"strings"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/peetzweg/matcrawl/internal/store"
)

// eventTime converts a Matrix origin_server_ts (milliseconds) into a UTC
// time.Time. Zero in, zero out.
func eventTime(evt *event.Event) time.Time {
	if evt.Timestamp == 0 {
		return time.Time{}
	}
	return time.UnixMilli(evt.Timestamp).UTC()
}

func rawJSON(evt *event.Event) string {
	buf, err := json.Marshal(evt)
	if err != nil {
		return ""
	}
	return string(buf)
}

// stateAccumulator collects mutations to a room as state events stream in.
// Both /sync state and the in-stream state events in timeline must flow
// through it so the final Room reflects the most recent value of each key.
type stateAccumulator struct {
	room store.Room
}

func newStateAccumulator(roomID id.RoomID) *stateAccumulator {
	return &stateAccumulator{room: store.Room{ID: string(roomID)}}
}

func (a *stateAccumulator) apply(evt *event.Event) {
	switch evt.Type {
	case event.StateRoomName:
		if err := evt.Content.ParseRaw(event.StateRoomName); err == nil {
			if name, ok := evt.Content.Parsed.(*event.RoomNameEventContent); ok && name != nil {
				a.room.Name = name.Name
			}
		}
	case event.StateTopic:
		if err := evt.Content.ParseRaw(event.StateTopic); err == nil {
			if topic := evt.Content.AsTopic(); topic != nil {
				a.room.Topic = topic.Topic
			}
		}
	case event.StateCanonicalAlias:
		if err := evt.Content.ParseRaw(event.StateCanonicalAlias); err == nil {
			if alias, ok := evt.Content.Parsed.(*event.CanonicalAliasEventContent); ok && alias != nil {
				a.room.CanonicalAlias = string(alias.Alias)
			}
		}
	case event.StateRoomAvatar:
		if err := evt.Content.ParseRaw(event.StateRoomAvatar); err == nil {
			if avatar, ok := evt.Content.Parsed.(*event.RoomAvatarEventContent); ok && avatar != nil {
				a.room.AvatarMXC = string(avatar.URL)
			}
		}
	case event.StateEncryption:
		if err := evt.Content.ParseRaw(event.StateEncryption); err == nil {
			if enc, ok := evt.Content.Parsed.(*event.EncryptionEventContent); ok && enc != nil {
				a.room.IsEncrypted = true
				a.room.EncryptionAlgorithm = string(enc.Algorithm)
			}
		}
	}
	ts := eventTime(evt)
	if !ts.IsZero() && ts.After(a.room.LastEventTS) {
		a.room.LastEventTS = ts
	}
}

func (a *stateAccumulator) result() store.Room {
	return a.room
}

// memberAccumulator collects m.room.member events. The state_key on a
// m.room.member event is the affected user_id; the membership field tracks
// transitions across join / leave / invite / ban / knock.
type memberAccumulator struct {
	roomID  id.RoomID
	members map[string]store.RoomMember
}

func newMemberAccumulator(roomID id.RoomID) *memberAccumulator {
	return &memberAccumulator{roomID: roomID, members: map[string]store.RoomMember{}}
}

func (m *memberAccumulator) apply(evt *event.Event) {
	if evt.Type != event.StateMember || evt.StateKey == nil {
		return
	}
	userID := *evt.StateKey
	if err := evt.Content.ParseRaw(event.StateMember); err == nil {
		if mem := evt.Content.AsMember(); mem != nil {
			m.members[userID] = store.RoomMember{
				RoomID:      string(m.roomID),
				UserID:      userID,
				DisplayName: mem.Displayname,
				AvatarMXC:   string(mem.AvatarURL),
				Membership:  string(mem.Membership),
			}
		}
	}
}

func (m *memberAccumulator) result() []store.RoomMember {
	out := make([]store.RoomMember, 0, len(m.members))
	for _, v := range m.members {
		out = append(out, v)
	}
	return out
}

// eventToMessage converts a single Matrix event into a store.Message row.
// Plaintext m.room.message events come back fully populated. Encrypted
// (m.room.encrypted) events come back with WasEncrypted=true and
// DecryptStatus="missing_keys" — PR 5's OlmMachine wiring upgrades them in
// place via a follow-up Upsert.
//
// Returns (zero, false) for event types that aren't archived as messages
// (state events, redactions, etc.).
func eventToMessage(roomID id.RoomID, evt *event.Event) (store.Message, bool) {
	switch evt.Type {
	case event.EventMessage, event.EventEncrypted:
		// fall through
	default:
		return store.Message{}, false
	}

	m := store.Message{
		RoomID:         string(roomID),
		EventID:        string(evt.ID),
		Sender:         string(evt.Sender),
		OriginServerTS: eventTime(evt),
		RawJSON:        rawJSON(evt),
	}

	if evt.Type == event.EventEncrypted {
		m.WasEncrypted = true
		m.DecryptStatus = "missing_keys"
		m.MsgType = "m.room.encrypted"
		return m, true
	}

	if err := evt.Content.ParseRaw(event.EventMessage); err != nil {
		// Body is unparseable; still record the event for completeness.
		m.MsgType = "m.text"
		return m, true
	}
	msg := evt.Content.AsMessage()
	if msg == nil {
		m.MsgType = "m.text"
		return m, true
	}
	m.MsgType = string(msg.MsgType)
	if m.MsgType == "" {
		m.MsgType = "m.text"
	}
	m.Body = msg.Body
	m.FormattedBody = msg.FormattedBody
	if rel := msg.GetRelatesTo(); rel != nil {
		if tid := rel.GetThreadParent(); tid != "" {
			m.ThreadID = string(tid)
		}
		if rid := rel.GetReplyTo(); rid != "" {
			m.ReplyToEventID = string(rid)
		}
	}
	if msg.URL != "" || msg.File != nil {
		attach := map[string]any{}
		if msg.URL != "" {
			attach["url"] = string(msg.URL)
		}
		if msg.Info != nil {
			attach["info"] = msg.Info
		}
		if msg.File != nil {
			attach["file"] = msg.File
		}
		if msg.FileName != "" {
			attach["filename"] = msg.FileName
		}
		if buf, err := json.Marshal(attach); err == nil {
			m.AttachmentsJSON = string(buf)
		}
	}
	return m, true
}

// trimToken strips whitespace and noisy quoting that homeservers sometimes
// reflect in pagination tokens.
func trimToken(s string) string {
	return strings.TrimSpace(s)
}
