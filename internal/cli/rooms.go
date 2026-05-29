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
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	return r.withStore(func(st *store.Store) error {
		rooms, err := st.ListRooms(r.ctx, store.RoomFilter{Limit: *limit, EncryptedOnly: *encrypted})
		if err != nil {
			return err
		}
		return r.print(rooms)
	})
}
