package notify

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Severity ranks how urgently a duty recipient must react.
type Severity string

const (
	SeverityNotice   Severity = "notice"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Rank allows routing rules to select every event at or above a threshold.
func (s Severity) Rank() int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityNotice:
		return 1
	default:
		return 0
	}
}

func (s Severity) Valid() bool { return s.Rank() > 0 }

// NotificationStatus is the delivery lifecycle state of one addressed notification.
type NotificationStatus string

const (
	// StatusPending waits for its next delivery attempt (including retry backoff).
	StatusPending NotificationStatus = "pending"
	// StatusSucceeded was accepted by the recipient endpoint.
	StatusSucceeded NotificationStatus = "succeeded"
	// StatusDead exhausted every retry and waits for manual intervention.
	StatusDead NotificationStatus = "dead"
)

// Notification is one addressed copy of a source event for one duty recipient.
// Its ID is derived from the source event and the recipient, so replaying the
// journal after a process restart never produces a different notification.
type Notification struct {
	ID              uuid.UUID          `json:"id"`
	SourceEventID   uuid.UUID          `json:"source_event_id"`
	ChamberID       string             `json:"chamber_id"`
	Sequence        uint64             `json:"sequence"`
	EventType       model.EventType    `json:"event_type"`
	EventOccurredAt time.Time          `json:"event_occurred_at"`
	Severity        Severity           `json:"severity"`
	Summary         string             `json:"summary"`
	RecipientID     string             `json:"recipient_id"`
	RecipientName   string             `json:"recipient_name"`
	RecipientKind   string             `json:"recipient_kind"`
	RecipientTarget string             `json:"recipient_target"`
	Status          NotificationStatus `json:"status"`
	AttemptCount    int                `json:"attempt_count"`
	MaxAttempts     int                `json:"max_attempts"`
	NextAttemptAt   time.Time          `json:"next_attempt_at"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
	LastError       string             `json:"last_error,omitempty"`
	Payload         json.RawMessage    `json:"payload,omitempty"`
}

// Pending reports whether the pump still owes a delivery attempt.
func (n Notification) Pending() bool { return n.Status == StatusPending }

// Due reports whether the notification may be attempted at the given time.
func (n Notification) Due(now time.Time) bool {
	return n.Status == StatusPending && !n.NextAttemptAt.After(now)
}

// Attempt records one delivery try; successful and failed attempts are both kept.
type Attempt struct {
	ID             uuid.UUID `json:"id"`
	NotificationID uuid.UUID `json:"notification_id"`
	AttemptNumber  int       `json:"attempt_number"`
	StartedAt      time.Time `json:"started_at"`
	DurationMS     int64     `json:"duration_ms"`
	Succeeded      bool      `json:"succeeded"`
	StatusCode     int       `json:"status_code,omitempty"`
	Error          string    `json:"error,omitempty"`
}
