package notify

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
)

// OutboxStore persists notifications independently of the safety journal.
// Notification writes never participate in safety-action transactions: a
// delivery failure must not roll back emergency stops or plan cancellations.
type OutboxStore interface {
	// SaveNotification upserts the notification, keyed by its deterministic ID.
	SaveNotification(context.Context, Notification) error
	// AppendAttempt records one delivery try.
	AppendAttempt(context.Context, Attempt) error
	Notification(ctx context.Context, id uuid.UUID) (Notification, bool, error)
	// HasAddress reports whether a notification for the source event already
	// exists for the recipient. It backs restart-safe idempotency.
	HasAddress(ctx context.Context, sourceEventID uuid.UUID, recipientID string) (bool, error)
	ListNotifications(ctx context.Context) ([]Notification, error)
	ListPending(ctx context.Context) ([]Notification, error)
	ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]Attempt, error)
}

// index is the in-memory view shared by the memory and file stores.
type index struct {
	notifications map[uuid.UUID]Notification
	addresses     map[[2]string]struct{} // source event id + recipient id
	attempts      map[uuid.UUID][]Attempt
}

func newIndex() *index {
	return &index{
		notifications: make(map[uuid.UUID]Notification),
		addresses:     make(map[[2]string]struct{}),
		attempts:      make(map[uuid.UUID][]Attempt),
	}
}

func (x *index) apply(notification Notification) {
	x.notifications[notification.ID] = notification
	x.addresses[[2]string{notification.SourceEventID.String(), notification.RecipientID}] = struct{}{}
}

func (x *index) list() []Notification {
	result := make([]Notification, 0, len(x.notifications))
	for _, notification := range x.notifications {
		result = append(result, notification)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].ID.String() < result[j].ID.String()
	})
	return result
}

func (x *index) pending() []Notification {
	var result []Notification
	for _, notification := range x.notifications {
		if notification.Pending() {
			result = append(result, notification)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].NextAttemptAt.Equal(result[j].NextAttemptAt) {
			return result[i].NextAttemptAt.Before(result[j].NextAttemptAt)
		}
		return result[i].ID.String() < result[j].ID.String()
	})
	return result
}

func (x *index) attemptsFor(notificationID uuid.UUID) []Attempt {
	result := append([]Attempt(nil), x.attempts[notificationID]...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].AttemptNumber != result[j].AttemptNumber {
			return result[i].AttemptNumber < result[j].AttemptNumber
		}
		return result[i].ID.String() < result[j].ID.String()
	})
	return result
}

// MemoryStore keeps the outbox in process memory for tests and ephemeral runs.
type MemoryStore struct {
	mu sync.RWMutex
	*index
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{index: newIndex()}
}

func (s *MemoryStore) SaveNotification(_ context.Context, notification Notification) error {
	s.mu.Lock()
	s.apply(notification)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) AppendAttempt(_ context.Context, attempt Attempt) error {
	s.mu.Lock()
	s.attempts[attempt.NotificationID] = append(s.attempts[attempt.NotificationID], attempt)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Notification(_ context.Context, id uuid.UUID) (Notification, bool, error) {
	s.mu.RLock()
	notification, ok := s.notifications[id]
	s.mu.RUnlock()
	return notification, ok, nil
}

func (s *MemoryStore) HasAddress(_ context.Context, sourceEventID uuid.UUID, recipientID string) (bool, error) {
	s.mu.RLock()
	_, ok := s.addresses[[2]string{sourceEventID.String(), recipientID}]
	s.mu.RUnlock()
	return ok, nil
}

func (s *MemoryStore) ListNotifications(_ context.Context) ([]Notification, error) {
	s.mu.RLock()
	result := s.list()
	s.mu.RUnlock()
	return result, nil
}

func (s *MemoryStore) ListPending(_ context.Context) ([]Notification, error) {
	s.mu.RLock()
	result := s.pending()
	s.mu.RUnlock()
	return result, nil
}

func (s *MemoryStore) ListAttempts(_ context.Context, notificationID uuid.UUID) ([]Attempt, error) {
	s.mu.RLock()
	result := s.index.attemptsFor(notificationID)
	s.mu.RUnlock()
	return result, nil
}

// NewRuntimeStore selects durable file storage when a directory is configured.
func NewRuntimeStore(dataDirectory string) (OutboxStore, error) {
	if dataDirectory == "" {
		return NewMemoryStore(), nil
	}
	return NewFileStore(dataDirectory)
}
