package notify

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RecipientKind selects the delivery transport for a duty endpoint.
const (
	KindWebhook = "webhook"
	KindLog     = "log"
)

// Recipient is one configured on-call endpoint.
type Recipient struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Target string `json:"target"`
}

// Route delivers events for a chamber at or above MinSeverity to RecipientID.
// ChamberID "*" applies to every chamber that has no explicit rule.
type Route struct {
	ChamberID   string   `json:"chamber_id"`
	MinSeverity Severity `json:"min_severity"`
	RecipientID string   `json:"recipient_id"`
}

// Config is the duty routing table loaded at startup.
type Config struct {
	Recipients []Recipient `json:"recipients"`
	Routes     []Route     `json:"routes"`
}

// DefaultConfig returns the built-in duty table used without an override file.
func DefaultConfig() Config {
	return Config{
		Recipients: []Recipient{
			{ID: "duty-critical", Name: "Duty engineer (24/7)", Kind: KindLog, Target: "duty-critical"},
			{ID: "duty-ops", Name: "Operations desk", Kind: KindLog, Target: "duty-ops"},
		},
		Routes: []Route{
			{ChamberID: "*", MinSeverity: SeverityCritical, RecipientID: "duty-critical"},
			{ChamberID: "*", MinSeverity: SeverityNotice, RecipientID: "duty-ops"},
		},
	}
}

// LoadConfig reads an optional JSON override. A missing file is not an error:
// the built-in default duty table is used instead.
func LoadConfig(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return DefaultConfig(), nil
	}
	data, err := os.ReadFile(filepath.Clean(path))
	if errors.Is(err, os.ErrNotExist) {
		return DefaultConfig(), nil
	}
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, err
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if len(c.Recipients) == 0 {
		return errors.New("notify config requires at least one recipient")
	}
	known := make(map[string]Recipient, len(c.Recipients))
	for _, recipient := range c.Recipients {
		if recipient.ID == "" || recipient.Name == "" {
			return errors.New("notify recipient requires id and name")
		}
		if _, dup := known[recipient.ID]; dup {
			return errors.New("duplicate notify recipient id " + recipient.ID)
		}
		switch recipient.Kind {
		case KindWebhook, KindLog:
		default:
			return errors.New("unsupported notify recipient kind " + recipient.Kind)
		}
		if strings.TrimSpace(recipient.Target) == "" {
			return errors.New("notify recipient " + recipient.ID + " requires a target")
		}
		known[recipient.ID] = recipient
	}
	if len(c.Routes) == 0 {
		return errors.New("notify config requires at least one route")
	}
	for _, route := range c.Routes {
		if route.ChamberID == "" {
			return errors.New("notify route requires chamber id (use * for any chamber)")
		}
		if !route.MinSeverity.Valid() {
			return errors.New("notify route for " + route.ChamberID + " has invalid min_severity")
		}
		if _, ok := known[route.RecipientID]; !ok {
			return errors.New("notify route points at unknown recipient " + route.RecipientID)
		}
	}
	return nil
}

// Resolve returns the recipients that should receive an event for a chamber at
// the given severity. Explicit chamber rules and wildcard rules both apply;
// each recipient is returned at most once.
func (c Config) Resolve(chamberID string, severity Severity) []Recipient {
	if !severity.Valid() {
		return nil
	}
	matched := make(map[string]struct{})
	for _, route := range c.Routes {
		if severity.Rank() < route.MinSeverity.Rank() {
			continue
		}
		if route.ChamberID == chamberID || route.ChamberID == "*" {
			matched[route.RecipientID] = struct{}{}
		}
	}
	byID := make(map[string]Recipient, len(c.Recipients))
	for _, recipient := range c.Recipients {
		byID[recipient.ID] = recipient
	}
	result := make([]Recipient, 0, len(matched))
	for id := range matched {
		if recipient, ok := byID[id]; ok {
			result = append(result, recipient)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
