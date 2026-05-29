package cli

import (
	"flag"
	"io"

	"github.com/peetzweg/matcrawl/internal/matrix"
	"github.com/peetzweg/matcrawl/internal/store"
)

type doctorReport struct {
	Matrix          matrix.Report `json:"matrix"`
	DBPath          string        `json:"db_path"`
	Rooms           int           `json:"rooms"`
	EncryptedRooms  int           `json:"encrypted_rooms"`
	Messages        int           `json:"messages"`
	DecryptFailures int           `json:"decrypt_failures"`
}

func (r *runtime) runDoctor(args []string) error {
	fs := flag.NewFlagSet("matcrawl doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return usageErr(err)
	}

	report := doctorReport{
		Matrix: matrix.Probe(r.ctx, sessionPath(r.dbPath)),
		DBPath: r.dbPath,
	}

	// Store inspection is best-effort: doctor should still produce output on a
	// cold install where the db hasn't been created yet.
	if st, err := store.Open(r.ctx, r.dbPath); err == nil {
		if status, sErr := st.Status(r.ctx); sErr == nil {
			report.Rooms = status.Rooms
			report.EncryptedRooms = status.EncryptedRooms
			report.Messages = status.Messages
			report.DecryptFailures = status.DecryptFailures
		}
		_ = st.Close()
	}

	return r.print(report)
}
