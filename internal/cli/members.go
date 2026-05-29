package cli

import (
	"errors"
	"flag"
	"io"

	"github.com/peetzweg/matcrawl/internal/store"
)

func (r *runtime) runMembers(args []string) error {
	fs := flag.NewFlagSet("matcrawl members", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	room := fs.String("room", "", "")
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}
	if *room == "" {
		return usageErr(errors.New("--room is required"))
	}
	return r.withStore(func(st *store.Store) error {
		members, err := st.ListMembers(r.ctx, *room)
		if err != nil {
			return err
		}
		return r.print(members)
	})
}
