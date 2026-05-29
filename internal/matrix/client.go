package matrix

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// New constructs a mautrix.Client from a persisted session. It does not call
// the network — use Verify to confirm the credentials are still valid.
func New(s Session) (*mautrix.Client, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	client, err := mautrix.NewClient(s.Homeserver, id.UserID(s.UserID), s.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("new mautrix client: %w", err)
	}
	client.DeviceID = id.DeviceID(s.DeviceID)
	client.UserAgent = "matcrawl"
	return client, nil
}

// Verify runs /account/whoami and checks the server-side identity agrees with
// the local session. Returns the resolved (user_id, device_id) for callers
// that want to capture any server-side correction (e.g. device_id renaming).
func Verify(ctx context.Context, s Session) (id.UserID, id.DeviceID, error) {
	client, err := New(s)
	if err != nil {
		return "", "", err
	}
	resp, err := client.Whoami(ctx)
	if err != nil {
		return "", "", fmt.Errorf("whoami: %w", err)
	}
	return resp.UserID, resp.DeviceID, nil
}
