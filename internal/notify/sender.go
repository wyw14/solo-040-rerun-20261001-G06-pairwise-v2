package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Message is the wire payload handed to a duty recipient. It always carries
// the originating event id so receivers can deduplicate and cross-reference.
type Message struct {
	NotificationID string          `json:"notification_id"`
	SourceEventID  string          `json:"source_event_id"`
	ChamberID      string          `json:"chamber_id"`
	Sequence       uint64          `json:"sequence"`
	EventType      string          `json:"event_type"`
	Severity       Severity        `json:"severity"`
	Summary        string          `json:"summary"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Attempt        int             `json:"attempt"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

// Result reports the outcome of one delivery attempt.
type Result struct {
	StatusCode int
}

// Sender delivers one message to a recipient transport.
type Sender interface {
	Send(ctx context.Context, recipient Recipient, message Message) (Result, error)
}

// HTTPSender posts JSON to webhook recipients. 2xx responses succeed; anything
// else is a retryable failure (network errors included).
type HTTPSender struct {
	client  *http.Client
	timeout time.Duration
}

func NewHTTPSender(timeout time.Duration) *HTTPSender {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPSender{client: &http.Client{Timeout: timeout}, timeout: timeout}
}

func (s *HTTPSender) Send(ctx context.Context, recipient Recipient, message Message) (Result, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return Result{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, recipient.Target, bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("X-LockFlow-Event-ID", message.SourceEventID)
	request.Header.Set("X-LockFlow-Notification-ID", message.NotificationID)
	response, err := s.client.Do(request)
	if err != nil {
		return Result{}, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Result{StatusCode: response.StatusCode}, fmt.Errorf("webhook returned status %d", response.StatusCode)
	}
	return Result{StatusCode: response.StatusCode}, nil
}

// LogSender writes notifications to structured logs. It is the default
// transport when no external webhook is configured and never fails, so a
// missing integration cannot stall safety notifications.
type LogSender struct {
	logger *slog.Logger
}

func NewLogSender(logger *slog.Logger) *LogSender {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogSender{logger: logger}
}

func (s *LogSender) Send(_ context.Context, recipient Recipient, message Message) (Result, error) {
	level := slog.LevelInfo
	switch message.Severity {
	case SeverityCritical:
		level = slog.LevelError
	case SeverityWarning:
		level = slog.LevelWarn
	}
	s.logger.Log(context.Background(), level, "duty notification",
		"recipient", recipient.ID, "target", recipient.Target,
		"chamber", message.ChamberID, "severity", string(message.Severity),
		"event_type", message.EventType, "source_event_id", message.SourceEventID,
		"notification_id", message.NotificationID, "attempt", message.Attempt,
		"summary", message.Summary,
	)
	return Result{StatusCode: http.StatusOK}, nil
}

// Registry selects the transport for a recipient kind.
type Registry struct {
	webhook Sender
	log     Sender
}

func NewRegistry(webhook Sender, log Sender) *Registry {
	if webhook == nil {
		webhook = NewHTTPSender(0)
	}
	if log == nil {
		log = NewLogSender(nil)
	}
	return &Registry{webhook: webhook, log: log}
}

func (r *Registry) SenderFor(kind string) (Sender, error) {
	switch strings.TrimSpace(kind) {
	case KindWebhook:
		return r.webhook, nil
	case KindLog:
		return r.log, nil
	default:
		return nil, fmt.Errorf("unsupported recipient kind %q", kind)
	}
}
