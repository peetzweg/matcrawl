package cli

import (
	"flag"
	"io"

	"github.com/peetzweg/matcrawl/internal/store"
)

func (r *runtime) runRooms(args []string) error {
	fs := flag.NewFlagSet("matcrawl rooms", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	limit := fs.Int("limit", 50, "")
	encrypted := fs.Bool("encrypted-only", false, "")
	all := fs.Bool("all", false, "")
	search := fs.String("search", "", "")
	space := fs.String("space", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *all {
		*limit = 0
	}
	return r.withStore(func(st *store.Store) error {
		rooms, err := st.ListRooms(r.ctx, store.RoomFilter{
			Limit:         *limit,
			EncryptedOnly: *encrypted,
			Search:        *search,
			SpaceID:       *space,
		})
		if err != nil {
			return err
		}
		return r.print(rooms)
	})
}

// runSpaces lists the spaces in the archive with their direct children.
func (r *runtime) runSpaces(args []string) error {
	fs := flag.NewFlagSet("matcrawl spaces", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	type spaceOut struct {
		ID       string          `json:"id"`
		Name     string          `json:"name,omitempty"`
		Topic    string          `json:"topic,omitempty"`
		Children []store.RoomRef `json:"children"`
	}
	return r.withStore(func(st *store.Store) error {
		spaces, err := st.ListRooms(r.ctx, store.RoomFilter{SpacesOnly: true})
		if err != nil {
			return err
		}
		children, err := st.SpaceChildren(r.ctx)
		if err != nil {
			return err
		}
		out := make([]spaceOut, 0, len(spaces))
		for _, sp := range spaces {
			out = append(out, spaceOut{ID: sp.ID, Name: sp.DisplayName, Topic: sp.Topic, Children: children[sp.ID]})
		}
		return r.print(out)
	})
}
