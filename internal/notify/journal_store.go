package notify

import (
	"context"
	"log/slog"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// JournalHook consumes events once they are durable.
type JournalHook interface {
	Ingest(context.Context, model.Event) error
}

// JournalStore wraps the safety journal so every successfully appended event
// is offered to the notification outbox. The hook runs strictly AFTER the
// append returns: a notification can never exist for an event that was not
// persisted, and a notification-side error is logged and swallowed so it can
// never roll back an emergency stop, cancellation, or other safety action.
// Startup journal replay (BootstrapFromJournal) covers any event the hook
// could not enqueue.
type JournalStore struct {
	journal.Store
	hook   JournalHook
	logger *slog.Logger
}

func NewJournalStore(inner journal.Store, logger *slog.Logger) *JournalStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &JournalStore{Store: inner, logger: logger}
}

func (s *JournalStore) BindHook(hook JournalHook) { s.hook = hook }

func (s *JournalStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	// The inner append is the safety-critical write; it must complete first
	// and its result must reach the caller untouched.
	persisted, err := s.Store.Append(ctx, event)
	if err != nil {
		return persisted, err
	}
	if s.hook == nil {
		return persisted, nil
	}
	// Detach from the request context: the safety action is already done, so
	// an HTTP client disconnect must not prevent the notification enqueue.
	if hookErr := s.hook.Ingest(context.WithoutCancel(ctx), persisted); hookErr != nil {
		s.logger.Error("notification ingest failed; event will be picked up at next startup",
			"event_id", persisted.ID, "event_type", persisted.Type, "error", hookErr)
	}
	return persisted, nil
}
