package notify

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigRoutesEverySeverity(t *testing.T) {
	config := DefaultConfig()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	critical := config.Resolve("chamber-9", SeverityCritical)
	if len(critical) < 1 {
		t.Fatal("critical event must reach at least one duty recipient")
	}
	notice := config.Resolve("chamber-9", SeverityNotice)
	if len(notice) < 1 {
		t.Fatal("notice event must reach at least one duty recipient")
	}
}

func TestLoadConfigMissingFileFallsBackToDefault(t *testing.T) {
	config, err := LoadConfig(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Recipients) == 0 {
		t.Fatal("expected built-in default config")
	}
}

func TestLoadConfigRejectsUnknownRouteRecipient(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.json")
	if err := os.WriteFile(path, []byte(`{
		"recipients":[{"id":"r1","name":"R1","kind":"log","target":"r1"}],
		"routes":[{"chamber_id":"*","min_severity":"warning","recipient_id":"missing"}]
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected validation error for unknown route recipient")
	}
}

func TestLoadConfigWebhookOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notify.json")
	if err := os.WriteFile(path, []byte(`{
		"recipients":[
			{"id":"hook","name":"Pager","kind":"webhook","target":"https://example.test/alert"},
			{"id":"desk","name":"Desk","kind":"log","target":"desk"}
		],
		"routes":[
			{"chamber_id":"chamber-1","min_severity":"critical","recipient_id":"hook"},
			{"chamber_id":"*","min_severity":"notice","recipient_id":"desk"}
		]
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	got := config.Resolve("chamber-1", SeverityCritical)
	ids := map[string]bool{}
	for _, r := range got {
		ids[r.ID] = true
	}
	if !ids["hook"] || !ids["desk"] {
		t.Fatalf("chamber-1 critical must hit explicit webhook and wildcard desk, got %v", ids)
	}
	got = config.Resolve("chamber-2", SeverityCritical)
	ids = map[string]bool{}
	for _, r := range got {
		ids[r.ID] = true
	}
	if ids["hook"] || !ids["desk"] {
		t.Fatalf("chamber-2 must not hit the chamber-1 webhook, got %v", ids)
	}
}
