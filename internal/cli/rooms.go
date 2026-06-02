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
		})
		if err != nil {
			return err
		}
		return r.print(rooms)
	})
}
