package matrix

import (
	"encoding/json"
	"errors"
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

// parseContent parses evt's content as t, treating already-parsed content as
// success: mautrix hands back decrypted events pre-parsed, and an event can
// be inspected more than once while being classified.
func parseContent(evt *event.Event, t event.Type) error {
	if err := evt.Content.ParseRaw(t); err != nil && !errors.Is(err, event.ErrContentAlreadyParsed) {
		return err
	}
	return nil
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
	room     store.Room
	children map[string]store.SpaceChild
}

func newStateAccumulator(roomID id.RoomID) *stateAccumulator {
	return &stateAccumulator{room: store.Room{ID: string(roomID)}, children: map[string]store.SpaceChild{}}
}

func (a *stateAccumulator) apply(evt *event.Event) {
	if evt.StateKey == nil {
		// Timeline events only bump the activity timestamp below.
		a.bumpTS(evt)
		return
	}
	switch evt.Type {
	case event.StateCreate:
		if err := parseContent(evt, event.StateCreate); err == nil {
			if c, ok := evt.Content.Parsed.(*event.CreateEventContent); ok && c != nil {
				a.room.RoomType = string(c.Type)
				a.room.RoomVersion = string(c.RoomVersion)
				a.room.Creator = string(evt.Sender)
				if c.Creator != "" {
					a.room.Creator = string(c.Creator)
				}
				a.room.CreatedAt = eventTime(evt)
				if c.Predecessor != nil {
					a.room.PredecessorRoomID = string(c.Predecessor.RoomID)
				}
			}
		}
	case event.StateTombstone:
		if err := parseContent(evt, event.StateTombstone); err == nil {
			if t, ok := evt.Content.Parsed.(*event.TombstoneEventContent); ok && t != nil {
				a.room.ReplacementRoomID = string(t.ReplacementRoom)
			}
		}
	case event.StatePowerLevels:
		if len(evt.Content.VeryRaw) > 0 && json.Valid(evt.Content.VeryRaw) {
			a.room.PowerLevelsJSON = string(evt.Content.VeryRaw)
		}
	case event.StateSpaceChild:
		child := store.SpaceChild{SpaceID: a.room.ID, ChildID: *evt.StateKey}
		if err := parseContent(evt, event.StateSpaceChild); err == nil {
			if c, ok := evt.Content.Parsed.(*event.SpaceChildEventContent); ok && c != nil {
				child.Via, child.Order, child.Suggested = c.Via, c.Order, c.Suggested
			}
		}
		// Per spec a child with no via is not part of the space.
		child.Removed = len(child.Via) == 0
		a.children[child.ChildID] = child
	}
	switch evt.Type {
	case event.StateRoomName:
		if err := parseContent(evt, event.StateRoomName); err == nil {
			if name, ok := evt.Content.Parsed.(*event.RoomNameEventContent); ok && name != nil {
				a.room.Name = name.Name
			}
		}
	case event.StateTopic:
		if err := parseContent(evt, event.StateTopic); err == nil {
			if topic := evt.Content.AsTopic(); topic != nil {
				a.room.Topic = topic.Topic
			}
		}
	case event.StateCanonicalAlias:
		if err := parseContent(evt, event.StateCanonicalAlias); err == nil {
			if alias, ok := evt.Content.Parsed.(*event.CanonicalAliasEventContent); ok && alias != nil {
				a.room.CanonicalAlias = string(alias.Alias)
			}
		}
	case event.StateRoomAvatar:
		if err := parseContent(evt, event.StateRoomAvatar); err == nil {
			if avatar, ok := evt.Content.Parsed.(*event.RoomAvatarEventContent); ok && avatar != nil {
				a.room.AvatarMXC = string(avatar.URL)
			}
		}
	case event.StateEncryption:
		if err := parseContent(evt, event.StateEncryption); err == nil {
			if enc, ok := evt.Content.Parsed.(*event.EncryptionEventContent); ok && enc != nil {
				a.room.IsEncrypted = true
				a.room.EncryptionAlgorithm = string(enc.Algorithm)
			}
		}
	}
	a.bumpTS(evt)
}

func (a *stateAccumulator) bumpTS(evt *event.Event) {
	ts := eventTime(evt)
	if !ts.IsZero() && ts.After(a.room.LastEventTS) {
		a.room.LastEventTS = ts
	}
}

func (a *stateAccumulator) result() store.Room {
	return a.room
}

func (a *stateAccumulator) spaceChildren() []store.SpaceChild {
	out := make([]store.SpaceChild, 0, len(a.children))
	for _, c := range a.children {
		out = append(out, c)
	}
	return out
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
	if err := parseContent(evt, event.StateMember); err == nil {
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

// joinCount returns the number of currently-joined members in the accumulator.
// Used to fill Room.MemberCount when the server's LazyLoadSummary doesn't
// include JoinedMemberCount (frequent on non-lazy-load /sync responses).
func (m *memberAccumulator) joinCount() int {
	n := 0
	for _, v := range m.members {
		if v.Membership == "join" {
			n++
		}
	}
	return n
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
	switch evt.Type.Type {
	case event.EventMessage.Type, event.EventEncrypted.Type, event.EventSticker.Type:
		// fall through
	default:
		if evt.StateKey != nil {
			return store.Message{}, false
		}
		body := fallbackBody(evt)
		if body == "" {
			return store.Message{}, false
		}
		return store.Message{
			RoomID:         string(roomID),
			EventID:        string(evt.ID),
			Sender:         string(evt.Sender),
			OriginServerTS: eventTime(evt),
			MsgType:        evt.Type.Type,
			Body:           body,
			RawJSON:        rawJSON(evt),
		}, true
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

	defaultType := "m.text"
	if evt.Type.Type == event.EventSticker.Type {
		defaultType = "m.sticker"
	}
	if err := parseContent(evt, evt.Type); err != nil {
		// Body is unparseable; still record the event for completeness.
		m.MsgType = defaultType
		return m, true
	}
	msg := evt.Content.AsMessage()
	if msg == nil {
		m.MsgType = defaultType
		return m, true
	}
	m.MsgType = string(msg.MsgType)
	if m.MsgType == "" {
		m.MsgType = defaultType
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

// appendEvent classifies one timeline event into b: a message, or a relation
// (edit, reaction, redaction) pointing at another event. State events are
// handled by the accumulators and ignored here; so are event types that carry
// nothing worth archiving (receipts, call signalling, poll votes, …).
func appendEvent(b *store.Batch, roomID id.RoomID, evt *event.Event) {
	if evt.StateKey != nil {
		return
	}
	if rb := evt.Unsigned.RedactedBecause; rb != nil {
		// The server already stripped this event; remember who deleted it.
		b.Redactions = append(b.Redactions, redactionFrom(roomID, rb, evt.ID))
	}
	switch evt.Type.Type {
	case event.EventRedaction.Type:
		if target := redactTarget(evt); target != "" {
			b.Redactions = append(b.Redactions, redactionFrom(roomID, evt, target))
		}
		return
	case event.EventReaction.Type:
		if r, ok := reactionFrom(roomID, evt); ok {
			b.Reactions = append(b.Reactions, r)
		}
		return
	case event.EventMessage.Type:
		if e, ok := editFrom(roomID, evt); ok {
			b.Edits = append(b.Edits, e)
			return
		}
	}
	if m, ok := eventToMessage(roomID, evt); ok {
		b.Messages = append(b.Messages, m)
	}
}

func redactionFrom(roomID id.RoomID, evt *event.Event, target id.EventID) store.Redaction {
	r := store.Redaction{
		RoomID:         string(roomID),
		EventID:        string(evt.ID),
		TargetEventID:  string(target),
		Sender:         string(evt.Sender),
		OriginServerTS: eventTime(evt),
	}
	if reason, ok := evt.Content.Raw["reason"].(string); ok {
		r.Reason = reason
	}
	return r
}

func redactsFromContent(evt *event.Event) id.EventID {
	if v, ok := evt.Content.Raw["redacts"].(string); ok {
		return id.EventID(v)
	}
	return ""
}

// redactTarget reads the redacted event ID: in content as of room v11,
// top-level before that.
func redactTarget(evt *event.Event) id.EventID {
	if t := redactsFromContent(evt); t != "" {
		return t
	}
	return evt.Redacts
}

func reactionFrom(roomID id.RoomID, evt *event.Event) (store.Reaction, bool) {
	if err := parseContent(evt, event.EventReaction); err != nil {
		return store.Reaction{}, false
	}
	content := evt.Content.AsReaction()
	if content == nil || content.RelatesTo.GetAnnotationID() == "" {
		return store.Reaction{}, false
	}
	return store.Reaction{
		RoomID:         string(roomID),
		EventID:        string(evt.ID),
		TargetEventID:  string(content.RelatesTo.EventID),
		Sender:         string(evt.Sender),
		Key:            content.RelatesTo.Key,
		OriginServerTS: eventTime(evt),
	}, true
}

// editFrom recognises an m.replace edit. The new text lives in m.new_content;
// the top-level body is a "* text" fallback for clients without edit support.
func editFrom(roomID id.RoomID, evt *event.Event) (store.Edit, bool) {
	if err := parseContent(evt, event.EventMessage); err != nil {
		return store.Edit{}, false
	}
	msg := evt.Content.AsMessage()
	if msg == nil {
		return store.Edit{}, false
	}
	target := msg.RelatesTo.GetReplaceID()
	if target == "" {
		return store.Edit{}, false
	}
	e := store.Edit{
		RoomID:         string(roomID),
		EventID:        string(evt.ID),
		TargetEventID:  string(target),
		Sender:         string(evt.Sender),
		OriginServerTS: eventTime(evt),
		MsgType:        string(msg.MsgType),
		Body:           strings.TrimPrefix(msg.Body, "* "),
		FormattedBody:  strings.TrimPrefix(msg.FormattedBody, "* "),
		RawJSON:        rawJSON(evt),
	}
	if nc := msg.NewContent; nc != nil {
		e.MsgType, e.Body, e.FormattedBody = string(nc.MsgType), nc.Body, nc.FormattedBody
	}
	return e, true
}

// fallbackBody pulls displayable text out of event types matcrawl doesn't
// model explicitly (polls, custom bridge events): a plain "body", the
// extensible-events text fallback, or a poll question.
func fallbackBody(evt *event.Event) string {
	raw := evt.Content.Raw
	if v, ok := raw["body"].(string); ok && v != "" {
		return v
	}
	if v, ok := raw["org.matrix.msc1767.text"].(string); ok && v != "" {
		return v
	}
	for _, key := range []string{"org.matrix.msc3381.poll.start", "m.poll"} {
		poll, _ := raw[key].(map[string]any)
		question, _ := poll["question"].(map[string]any)
		for _, qk := range []string{"org.matrix.msc1767.text", "m.text", "body"} {
			if v, ok := question[qk].(string); ok && v != "" {
				return v
			}
		}
	}
	return ""
}
