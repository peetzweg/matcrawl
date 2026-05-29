package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/peetzweg/matcrawl/internal/store"
)

func (r *runtime) runMessages(args []string) error {
	filter, err := r.messageFilter("matcrawl messages", args, false)
	if err != nil {
		return err
	}
	return r.withStore(func(st *store.Store) error {
		messages, err := st.Messages(r.ctx, filter)
		if err != nil {
			return err
		}
		return r.print(messages)
	})
}

func (r *runtime) runSearch(args []string) error {
	filter, err := r.messageFilter("matcrawl search", args, true)
	if err != nil {
		return err
	}
	return r.withStore(func(st *store.Store) error {
		messages, err := st.Search(r.ctx, filter)
		if err != nil {
			return err
		}
		return r.print(messages)
	})
}

func (r *runtime) messageFilter(name string, args []string, requireQuery bool) (store.MessageFilter, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var filter store.MessageFilter
	fs.StringVar(&filter.RoomID, "room", "", "")
	fs.StringVar(&filter.Sender, "sender", "", "")
	fs.IntVar(&filter.Limit, "limit", 50, "")
	after := fs.String("after", "", "")
	before := fs.String("before", "", "")
	fs.BoolVar(&filter.HasMedia, "media", false, "")
	fs.BoolVar(&filter.Asc, "asc", false, "")
	if err := fs.Parse(args); err != nil {
		return filter, usageErr(err)
	}
	if requireQuery {
		if fs.NArg() != 1 {
			return filter, usageErr(errors.New("search takes exactly one query"))
		}
		filter.Query = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return filter, usageErr(errors.New("messages takes flags only"))
	}
	if *after != "" {
		t, err := parseDate(*after)
		if err != nil {
			return filter, usageErr(err)
		}
		filter.After = &t
	}
	if *before != "" {
		t, err := parseDate(*before)
		if err != nil {
			return filter, usageErr(err)
		}
		filter.Before = &t
	}
	return filter, nil
}

func parseDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date %q (use RFC3339 or YYYY-MM-DD)", value)
}
