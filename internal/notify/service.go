package notify

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// namespaceNotifications scopes deterministic notification UUIDs.
var namespaceNotifications = uuid.NewSHA1(uuid.NameSpaceURL, []byte("lockflow.local/notify"))

const (
	defaultMaxAttempts = 6
	defaultSendTimeout = 5 * time.Second
	defaultBaseBackoff = 2 * time.Second
	defaultMaxBackoff  = 5 * time.Minute
	pumpWakeInterval   = time.Second
)

// JournalEvents is the slice of the safety journal the outbox needs at startup.
type JournalEvents interface {
	Chambers(context.Context) ([]string, error)
	Events(context.Context, string, uint64) ([]model.Event, error)
}

// Service converts safety-journal events into addressed notifications and
// drives their delivery. Persistence is deliberately separate from the safety
// journal: notification work is committed only after the safety event is
// durable, and a delivery failure never propagates back into a safety action.
type Service struct {
	store    OutboxStore
	config   Config
	registry *Registry
	logger   *slog.Logger
	now      func() time.Time

	maxAttempts int
	sendTimeout time.Duration
	baseBackoff time.Duration
	maxBackoff  time.Duration

	mu       sync.Mutex
	inflight map[uuid.UUID]struct{}
	wake     chan struct{}
}

func NewService(store OutboxStore, config Config, registry *Registry, now func() time.Time, logger *slog.Logger) *Service {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	if registry == nil {
		registry = NewRegistry(nil, nil)
	}
	return &Service{
		store: store, config: config, registry: registry, logger: logger, now: now,
		maxAttempts: defaultMaxAttempts, sendTimeout: defaultSendTimeout,
		baseBackoff: defaultBaseBackoff, maxBackoff: defaultMaxBackoff,
		inflight: make(map[uuid.UUID]struct{}), wake: make(chan struct{}, 1),
	}
}

// deterministicID makes a restart-safe identity: the same source event
// addressed to the same recipient always yields the same notification id.
func deterministicID(sourceEventID uuid.UUID, recipientID string) uuid.UUID {
	return uuid.NewSHA1(namespaceNotifications, []byte(sourceEventID.String()+"|"+recipientID))
}

// Ingest converts one journal event into one pending notification per matched
// duty recipient. It is idempotent for (source event, recipient) pairs and
// never returns an error to safety-action callers without the event already
// being durable — the journal decorator treats returned errors as best-effort.
func (s *Service) Ingest(ctx context.Context, event model.Event) error {
	severity, summary, include := classify(event)
	if !include {
		return nil
	}
	recipients := s.config.Resolve(event.ChamberID, severity)
	if len(recipients) == 0 {
		s.logger.Warn("no duty recipient matched notification",
			"chamber", event.ChamberID, "severity", string(severity), "event_type", event.Type)
		return nil
	}
	created := false
	for _, recipient := range recipients {
		exists, err := s.store.HasAddress(ctx, event.ID, recipient.ID)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		now := s.now().UTC()
		notification := Notification{
			ID:              deterministicID(event.ID, recipient.ID),
			SourceEventID:   event.ID,
			ChamberID:       event.ChamberID,
			Sequence:        event.Sequence,
			EventType:       event.Type,
			EventOccurredAt: event.OccurredAt,
			Severity:        severity,
			Summary:         summary,
			RecipientID:     recipient.ID,
			RecipientName:   recipient.Name,
			RecipientKind:   recipient.Kind,
			RecipientTarget: recipient.Target,
			Status:          StatusPending,
			MaxAttempts:     s.maxAttempts,
			NextAttemptAt:   now,
			CreatedAt:       now,
			UpdatedAt:       now,
			Payload:         append(json.RawMessage(nil), event.Payload...),
		}
		// SaveNotification upserts by deterministic ID; a concurrent replay
		// and live ingest of the same event converge on one record.
		if err := s.store.SaveNotification(ctx, notification); err != nil {
			return err
		}
		created = true
		s.logger.Info("notification queued",
			"notification_id", notification.ID, "source_event_id", event.ID,
			"chamber", event.ChamberID, "severity", string(severity), "recipient", recipient.ID)
	}
	if created {
		s.kick()
	}
	return nil
}

// BootstrapFromJournal backfills notifications for every already-persisted
// event. Ingest's idempotent (source event, recipient) guard makes this safe
// to run unconditionally at startup and after an upgrade.
func (s *Service) BootstrapFromJournal(ctx context.Context, journal JournalEvents) error {
	chambers, err := journal.Chambers(ctx)
	if err != nil {
		return err
	}
	for _, chamberID := range chambers {
		events, err := journal.Events(ctx, chamberID, 0)
		if err != nil {
			return err
		}
		for _, event := range events {
			if err := s.Ingest(ctx, event); err != nil {
				return err
			}
		}
	}
	return nil
}

// Run delivers due notifications until the context is cancelled. Safe to run
// as a single background goroutine; Ingest wakes it when work appears.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(pumpWakeInterval)
	defer ticker.Stop()
	for {
		if err := s.Pump(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("notification pump error", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.wake:
		}
	}
}

func (s *Service) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Pump attempts every notification whose backoff has elapsed. Exported so
// tests and an operator-triggered flush can run one synchronous pass.
func (s *Service) Pump(ctx context.Context) error {
	pending, err := s.store.ListPending(ctx)
	if err != nil {
		return err
	}
	for _, notification := range pending {
		if !notification.Due(s.now()) {
			continue
		}
		if !s.claim(notification.ID) {
			continue
		}
		s.deliver(ctx, notification)
	}
	return nil
}

func (s *Service) claim(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.inflight[id]; busy {
		return false
	}
	s.inflight[id] = struct{}{}
	return true
}

func (s *Service) release(id uuid.UUID) {
	s.mu.Lock()
	delete(s.inflight, id)
	s.mu.Unlock()
}

func (s *Service) deliver(parent context.Context, notification Notification) {
	defer s.release(notification.ID)
	sender, err := s.registry.SenderFor(notification.RecipientKind)
	if err != nil {
		// Configuration errors are persistent; fail fast to dead-letter.
		s.finishAttempt(parent, notification, s.now(), Result{}, err, true)
		return
	}
	attemptNumber := notification.AttemptCount + 1
	message := Message{
		NotificationID: notification.ID.String(),
		SourceEventID:  notification.SourceEventID.String(),
		ChamberID:      notification.ChamberID,
		Sequence:       notification.Sequence,
		EventType:      string(notification.EventType),
		Severity:       notification.Severity,
		Summary:        notification.Summary,
		OccurredAt:     notification.EventOccurredAt,
		Attempt:        attemptNumber,
		Payload:        notification.Payload,
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), s.sendTimeout)
	defer cancel()
	started := s.now()
	result, sendErr := sender.Send(ctx, Recipient{
		ID: notification.RecipientID, Name: notification.RecipientName,
		Kind: notification.RecipientKind, Target: notification.RecipientTarget,
	}, message)
	s.finishAttempt(parent, notification, started, result, sendErr, false)
}

// finishAttempt persists the delivery ledger entry and the next state.
func (s *Service) finishAttempt(ctx context.Context, notification Notification, started time.Time, result Result, sendErr error, permanent bool) {
	now := s.now().UTC()
	attempt := Attempt{
		ID:             uuid.New(),
		NotificationID: notification.ID,
		AttemptNumber:  notification.AttemptCount + 1,
		StartedAt:      started.UTC(),
		DurationMS:     now.Sub(started.UTC()).Milliseconds(),
		Succeeded:      sendErr == nil,
		StatusCode:     result.StatusCode,
	}
	if sendErr != nil {
		attempt.Error = sendErr.Error()
	}
	if err := s.store.AppendAttempt(context.WithoutCancel(ctx), attempt); err != nil {
		s.logger.Error("could not persist notification attempt", "notification_id", notification.ID, "error", err)
	}

	notification.AttemptCount++
	notification.UpdatedAt = now
	switch {
	case sendErr == nil:
		notification.Status = StatusSucceeded
		notification.LastError = ""
		notification.NextAttemptAt = time.Time{}
	case permanent || notification.AttemptCount >= notification.MaxAttempts:
		notification.Status = StatusDead
		notification.LastError = sendErr.Error()
		notification.NextAttemptAt = time.Time{}
		s.logger.Error("notification dead-lettered",
			"notification_id", notification.ID, "source_event_id", notification.SourceEventID,
			"recipient", notification.RecipientID, "attempts", notification.AttemptCount, "error", sendErr)
	default:
		notification.LastError = sendErr.Error()
		notification.NextAttemptAt = now.Add(s.backoff(notification.AttemptCount))
	}
	if err := s.store.SaveNotification(context.WithoutCancel(ctx), notification); err != nil {
		s.logger.Error("could not update notification state", "notification_id", notification.ID, "error", err)
	}
}

// backoff grows exponentially from the first retry: 2s, 4s, 8s, ... capped.
func (s *Service) backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := s.baseBackoff
	for shift := 1; shift < attempts; shift++ {
		delay *= 2
		if delay >= s.maxBackoff {
			return s.maxBackoff
		}
	}
	if delay > s.maxBackoff {
		return s.maxBackoff
	}
	return delay
}

// ReprocessDead returns a dead-lettered notification to the pending queue for
// another round of attempts, e.g. after an operator repairs a webhook.
func (s *Service) ReprocessDead(ctx context.Context, id uuid.UUID) (Notification, error) {
	notification, ok, err := s.store.Notification(ctx, id)
	if err != nil {
		return Notification{}, err
	}
	if !ok {
		return Notification{}, ErrNotificationNotFound
	}
	if notification.Status != StatusDead {
		return Notification{}, errors.New("only dead-lettered notifications can be reprocessed")
	}
	notification.Status = StatusPending
	notification.NextAttemptAt = s.now().UTC()
	notification.UpdatedAt = notification.NextAttemptAt
	if err := s.store.SaveNotification(ctx, notification); err != nil {
		return Notification{}, err
	}
	s.kick()
	return notification, nil
}

// Views ---------------------------------------------------------------------

// ErrNotificationNotFound is returned when an id does not address a known notification.
var ErrNotificationNotFound = errors.New("notification not found")

func (s *Service) NotificationView(ctx context.Context, id uuid.UUID) (Notification, bool, error) {
	return s.store.Notification(ctx, id)
}

func (s *Service) ListPending(ctx context.Context) ([]Notification, error) {
	return s.store.ListPending(ctx)
}

func (s *Service) ListAll(ctx context.Context) ([]Notification, error) {
	return s.store.ListNotifications(ctx)
}

func (s *Service) Attempts(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	return s.store.ListAttempts(ctx, id)
}
