package backup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/peetzweg/matcrawl/internal/store"
)

const formatVersion = 1

type Manifest struct {
	Format     int          `json:"format"`
	Encrypted  bool         `json:"encrypted"`
	Exported   time.Time    `json:"exported"`
	Recipients []string     `json:"recipients,omitempty"`
	Counts     Counts       `json:"counts"`
	Shards     []ShardEntry `json:"shards"`
}

type Counts struct {
	Rooms    int `json:"rooms"`
	Members  int `json:"members"`
	Messages int `json:"messages"`
}

type ShardEntry struct {
	Table  string `json:"table"`
	Path   string `json:"path"`
	Rows   int    `json:"rows"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type Result struct {
	Repo      string `json:"repo"`
	Changed   bool   `json:"changed"`
	Encrypted bool   `json:"encrypted"`
	Shards    int    `json:"shards"`
	Messages  int    `json:"messages"`
}

func Init(ctx context.Context, opts Options) (Config, string, error) {
	cfg, err := ResolveOptions(opts)
	if err != nil {
		return Config{}, "", err
	}
	recipient, err := EnsureIdentity(cfg.Identity)
	if err != nil {
		return Config{}, "", err
	}
	if len(cfg.Recipients) == 0 {
		cfg.Recipients = []string{recipient}
	}
	if err := SaveConfig(opts.ConfigPath, cfg); err != nil {
		return Config{}, "", err
	}
	if err := ensureRepo(ctx, cfg); err != nil {
		return Config{}, "", err
	}
	if err := writeBackupReadme(cfg.Repo); err != nil {
		return Config{}, "", err
	}
	_, err = commitAndPush(ctx, cfg, "docs: describe encrypted matcrawl backup", opts.Push)
	return cfg, recipient, err
}

func Push(ctx context.Context, st *store.Store, opts Options) (Result, error) {
	cfg, err := ResolveOptions(opts)
	if err != nil {
		return Result{}, err
	}
	if len(cfg.Recipients) == 0 {
		recipient, err := RecipientFromIdentity(cfg.Identity)
		if err != nil {
			return Result{}, err
		}
		cfg.Recipients = []string{recipient}
	}
	if err := ensureRepo(ctx, cfg); err != nil {
		return Result{}, err
	}
	if err := writeBackupReadme(cfg.Repo); err != nil {
		return Result{}, err
	}
	oldManifest, _ := readManifest(cfg.Repo)
	data, err := st.ExportAll(ctx)
	if err != nil {
		return Result{}, err
	}
	manifest, err := writeSnapshot(ctx, cfg, data, oldManifest)
	if err != nil {
		return Result{}, err
	}
	changed, err := commitAndPush(ctx, cfg, "sync: update encrypted matcrawl backup", opts.Push)
	if err != nil {
		return Result{}, err
	}
	return Result{Repo: cfg.Repo, Changed: changed, Encrypted: true, Shards: len(manifest.Shards), Messages: manifest.Counts.Messages}, nil
}

func Pull(ctx context.Context, st *store.Store, opts Options) (Result, error) {
	cfg, err := ResolveOptions(opts)
	if err != nil {
		return Result{}, err
	}
	if err := ensureRepo(ctx, cfg); err != nil {
		return Result{}, err
	}
	manifest, err := readManifest(cfg.Repo)
	if err != nil {
		return Result{}, err
	}
	data, err := readSnapshot(cfg, manifest)
	if err != nil {
		return Result{}, err
	}
	if err := data.Validate(); err != nil {
		return Result{}, err
	}
	if err := st.ImportSnapshot(ctx, data, "backup:"+cfg.Repo, manifest.Exported); err != nil {
		return Result{}, err
	}
	return Result{Repo: cfg.Repo, Changed: true, Encrypted: manifest.Encrypted, Shards: len(manifest.Shards), Messages: len(data.Messages)}, nil
}

func Status(ctx context.Context, opts Options) (Manifest, string, error) {
	cfg, err := ResolveOptions(opts)
	if err != nil {
		return Manifest{}, "", err
	}
	if err := ensureRepo(ctx, cfg); err != nil {
		return Manifest{}, "", err
	}
	manifest, err := readManifest(cfg.Repo)
	if err != nil {
		return Manifest{}, "", err
	}
	return manifest, cfg.Repo, nil
}

func writeSnapshot(ctx context.Context, cfg Config, data store.Snapshot, old Manifest) (Manifest, error) {
	_ = ctx
	recipients := normalizedStrings(cfg.Recipients)
	reuseEncrypted := sameStrings(old.Recipients, recipients)
	var shards []ShardEntry
	add := func(table, rel string, rows any) error {
		plaintext, count, err := encodeJSONL(rows)
		if err != nil {
			return err
		}
		entry, err := writeShard(cfg, old, table, rel, plaintext, count, reuseEncrypted)
		if err != nil {
			return err
		}
		shards = append(shards, entry)
		return nil
	}
	if err := add("rooms", "data/rooms.jsonl.gz.age", data.Rooms); err != nil {
		return Manifest{}, err
	}
	if err := add("members", "data/members.jsonl.gz.age", data.Members); err != nil {
		return Manifest{}, err
	}
	for _, shard := range messageShards(data.Messages) {
		if err := add("messages", shard.path, shard.messages); err != nil {
			return Manifest{}, err
		}
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].Path < shards[j].Path })
	manifest := Manifest{
		Format:     formatVersion,
		Encrypted:  true,
		Exported:   time.Now().UTC(),
		Recipients: recipients,
		Counts:     Counts{Rooms: len(data.Rooms), Members: len(data.Members), Messages: len(data.Messages)},
		Shards:     shards,
	}
	if equivalentManifest(old, manifest) {
		return old, nil
	}
	if err := removeStaleShards(cfg.Repo, shards); err != nil {
		return Manifest{}, err
	}
	if err := writeManifest(cfg.Repo, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func readSnapshot(cfg Config, manifest Manifest) (store.Snapshot, error) {
	if manifest.Format != formatVersion {
		return store.Snapshot{}, fmt.Errorf("unsupported backup format %d", manifest.Format)
	}
	data := store.Snapshot{SchemaVersion: 1, ExportedAt: manifest.Exported}
	for _, shard := range manifest.Shards {
		plaintext, err := decryptShardFile(cfg, shard)
		if err != nil {
			return store.Snapshot{}, err
		}
		if got := sha256Hex(plaintext); got != shard.SHA256 {
			return store.Snapshot{}, fmt.Errorf("backup shard hash mismatch for %s", shard.Path)
		}
		switch shard.Table {
		case "rooms":
			if err := decodeJSONL(plaintext, &data.Rooms); err != nil {
				return store.Snapshot{}, err
			}
		case "members":
			if err := decodeJSONL(plaintext, &data.Members); err != nil {
				return store.Snapshot{}, err
			}
		case "messages":
			var messages []store.Message
			if err := decodeJSONL(plaintext, &messages); err != nil {
				return store.Snapshot{}, err
			}
			data.Messages = append(data.Messages, messages...)
		default:
			return store.Snapshot{}, fmt.Errorf("unknown backup table %q", shard.Table)
		}
	}
	sort.Slice(data.Messages, func(i, j int) bool {
		if data.Messages[i].OriginServerTS.Equal(data.Messages[j].OriginServerTS) {
			return data.Messages[i].EventID < data.Messages[j].EventID
		}
		return data.Messages[i].OriginServerTS.Before(data.Messages[j].OriginServerTS)
	})
	return data, nil
}

func writeShard(cfg Config, old Manifest, table, rel string, plaintext []byte, rows int, reuseEncrypted bool) (ShardEntry, error) {
	hash := sha256Hex(plaintext)
	full, err := resolveShardPath(cfg.Repo, rel)
	if err != nil {
		return ShardEntry{}, err
	}
	if oldEntry, ok := old.entry(rel); reuseEncrypted && ok && oldEntry.SHA256 == hash {
		if info, err := os.Stat(full); err == nil {
			oldEntry.Bytes = info.Size()
			return oldEntry, nil
		}
	}
	encrypted, _, err := encryptShard(plaintext, cfg.Recipients)
	if err != nil {
		return ShardEntry{}, err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return ShardEntry{}, err
	}
	if err := os.WriteFile(full, encrypted, 0o600); err != nil {
		return ShardEntry{}, err
	}
	return ShardEntry{Table: table, Path: rel, Rows: rows, SHA256: hash, Bytes: int64(len(encrypted))}, nil
}

func decryptShardFile(cfg Config, shard ShardEntry) ([]byte, error) {
	full, err := resolveShardPath(cfg.Repo, shard.Path)
	if err != nil {
		return nil, err
	}
	ciphertext, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	return decryptShard(ciphertext, cfg.Identity)
}

func resolveShardPath(repo, rel string) (string, error) {
	clean := path.Clean(strings.TrimSpace(rel))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) {
		return "", fmt.Errorf("backup shard path escapes backup root: %s", rel)
	}
	if !strings.HasPrefix(clean, "data/") || !strings.HasSuffix(clean, ".age") {
		return "", fmt.Errorf("invalid backup shard path: %s", rel)
	}
	full := filepath.Join(repo, filepath.FromSlash(clean))
	root := filepath.Clean(filepath.Join(repo, "data"))
	parent := filepath.Clean(filepath.Dir(full))
	if parent != root && !strings.HasPrefix(parent, root+string(filepath.Separator)) {
		return "", fmt.Errorf("backup shard path escapes backup root: %s", rel)
	}
	return full, nil
}

func encodeJSONL(rows any) ([]byte, int, error) {
	value := reflect.ValueOf(rows)
	if value.Kind() != reflect.Slice {
		return nil, 0, fmt.Errorf("unsupported JSONL rows %T", rows)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < value.Len(); i++ {
		if err := enc.Encode(value.Index(i).Interface()); err != nil {
			return nil, 0, err
		}
	}
	return buf.Bytes(), value.Len(), nil
}

func decodeJSONL[T any](plaintext []byte, out *[]T) error {
	scanner := bufio.NewScanner(bytes.NewReader(plaintext))
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var value T
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return err
		}
		*out = append(*out, value)
	}
	return scanner.Err()
}

type messageShard struct {
	path     string
	messages []store.Message
}

func messageShards(messages []store.Message) []messageShard {
	buckets := map[string][]store.Message{}
	for _, message := range messages {
		t := message.OriginServerTS.UTC()
		year, month := "unknown", "00"
		if !t.IsZero() {
			year = fmt.Sprintf("%04d", t.Year())
			month = fmt.Sprintf("%02d", int(t.Month()))
		}
		rel := fmt.Sprintf("data/messages/%s/%s.jsonl.gz.age", year, month)
		buckets[rel] = append(buckets[rel], message)
	}
	paths := make([]string, 0, len(buckets))
	for p := range buckets {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := make([]messageShard, 0, len(paths))
	for _, p := range paths {
		values := buckets[p]
		sort.Slice(values, func(i, j int) bool {
			if values[i].OriginServerTS.Equal(values[j].OriginServerTS) {
				return values[i].EventID < values[j].EventID
			}
			return values[i].OriginServerTS.Before(values[j].OriginServerTS)
		})
		out = append(out, messageShard{path: p, messages: values})
	}
	return out
}

func readManifest(repo string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(repo, "manifest.json"))
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func writeManifest(repo string, manifest Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(repo, "manifest.json"), data, 0o600)
}

func (m Manifest) entry(p string) (ShardEntry, bool) {
	for _, shard := range m.Shards {
		if shard.Path == p {
			return shard, true
		}
	}
	return ShardEntry{}, false
}

func equivalentManifest(a, b Manifest) bool {
	if a.Format != b.Format || a.Encrypted != b.Encrypted || !sameStrings(a.Recipients, b.Recipients) || a.Counts != b.Counts || len(a.Shards) != len(b.Shards) {
		return false
	}
	for i := range a.Shards {
		left, right := a.Shards[i], b.Shards[i]
		left.Bytes, right.Bytes = 0, 0
		if left != right {
			return false
		}
	}
	return true
}

func normalizedStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	a, b = normalizedStrings(a), normalizedStrings(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func removeStaleShards(repo string, shards []ShardEntry) error {
	keep := map[string]struct{}{}
	for _, shard := range shards {
		keep[filepath.Clean(filepath.Join(repo, filepath.FromSlash(shard.Path)))] = struct{}{}
	}
	root := filepath.Join(repo, "data")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	var stale []string
	if err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(p, ".age") {
			return nil
		}
		clean := filepath.Clean(p)
		if _, ok := keep[clean]; ok {
			return nil
		}
		stale = append(stale, clean)
		return nil
	}); err != nil {
		return err
	}
	for _, p := range stale {
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	return nil
}
