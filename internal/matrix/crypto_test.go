package matrix

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsurePickleKeyGeneratesAndReuses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "pickle.key")

	first, err := EnsurePickleKey(path)
	if err != nil {
		t.Fatalf("EnsurePickleKey (generate): %v", err)
	}
	if len(first) != pickleKeyBytes {
		t.Fatalf("len(first) = %d, want %d", len(first), pickleKeyBytes)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("pickle key perms = %o, want 0o600", mode)
	}

	second, err := EnsurePickleKey(path)
	if err != nil {
		t.Fatalf("EnsurePickleKey (reuse): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Error("EnsurePickleKey regenerated instead of reusing the existing key")
	}
}

func TestEnsurePickleKeyRejectsWrongLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pickle.key")
	if err := os.WriteFile(path, []byte("too short"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := EnsurePickleKey(path)
	if err == nil {
		t.Fatal("expected error for wrong-length key, got nil")
	}
}

func TestDefaultCryptoPaths(t *testing.T) {
	paths := DefaultCryptoPaths("/home/u/.matcrawl/matcrawl.db")
	if paths.CryptoDB != "/home/u/.matcrawl/crypto.db" {
		t.Errorf("CryptoDB = %s", paths.CryptoDB)
	}
	if paths.PickleKey != "/home/u/.matcrawl/pickle.key" {
		t.Errorf("PickleKey = %s", paths.PickleKey)
	}
}
