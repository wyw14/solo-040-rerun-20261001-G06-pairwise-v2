package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// fakeSender records every delivery and can fail a configurable number of
// consecutive attempts before succeeding.
type fakeSender struct {
	mu       sync.Mutex
	messages []Message
	failLeft int
	failErr  error
}

func (f *fakeSender) Send(_ context.Context, _ Recipient, message Message) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, message)
	if f.failLeft > 0 {
		f.failLeft--
		return Result{StatusCode: 503}, f.failErr
	}
	return Result{StatusCode: 200}, nil
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

func testConfig() Config {
	return Config{
		Recipients: []Recipient{
			{ID: "crit", Name: "Critical desk", Kind: KindLog, Target: "crit"},
			{ID: "ops", Name: "Ops desk", Kind: KindLog, Target: "ops"},
			{ID: "c1-only", Name: "Chamber 1 lead", Kind: KindLog, Target: "c1"},
		},
		Routes: []Route{
			{ChamberID: "*", MinSeverity: SeverityCritical, RecipientID: "crit"},
			{ChamberID: "*", MinSeverity: SeverityNotice, RecipientID: "ops"},
			{ChamberID: "chamber-1", MinSeverity: SeverityWarning, RecipientID: "c1-only"},
		},
	}
}

func newTestService(t *testing.T, store OutboxStore, sender *fakeSender) (*Service, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
	if sender == nil {
		sender = &fakeSender{failErr: errors.New("boom")}
	}
	registry := NewRegistry(sender, sender)
	service := NewService(store, testConfig(), registry, clock.now, nil)
	service.baseBackoff = time.Second
	service.maxBackoff = time.Minute
	return service, clock
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func makeEvent(t *testing.T, chamber string, kind model.EventType) model.Event {
	t.Helper()
	event, err := model.NewEvent(chamber, kind, uuid.Nil, 0, map[string]any{"note": "test"}, time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	event.Sequence = 7
	return event
}

func TestIngestRoutesByChamberAndSeverity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service, _ := newTestService(t, store, nil)

	critical := makeEvent(t, "chamber-1", model.EventEmergencyStarted)
	if err := service.Ingest(ctx, critical); err != nil {
		t.Fatal(err)
	}
	all, err := store.ListNotifications(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// critical severity + explicit chamber-1 warning rule: crit, ops(critical>=notice), c1-only
	got := recipientsOf(all)
	if len(got) != 3 {
		t.Fatalf("expected 3 addressed notifications, got %d: %v", len(got), got)
	}
	for _, want := range []string{"crit", "ops", "c1-only"} {
		if !got[want] {
			t.Errorf("expected recipient %s to be addressed", want)
		}
	}
	for _, notification := range all {
		if notification.SourceEventID != critical.ID {
			t.Errorf("notification missing source event id: %+v", notification)
		}
		if notification.Severity != SeverityCritical || notification.ChamberID != "chamber-1" {
			t.Errorf("unexpected routing metadata: %+v", notification)
		}
	}

	notice := makeEvent(t, "chamber-2", model.EventPlanCancelled)
	if err := service.Ingest(ctx, notice); err != nil {
		t.Fatal(err)
	}
	all, _ = store.ListNotifications(ctx)
	chamber2 := recipientsFor(all, "chamber-2")
	if len(chamber2) != 1 || !chamber2["ops"] {
		t.Fatalf("chamber-2 notice should reach ops only, got %v", chamber2)
	}
}

func recipientsOf(items []Notification) map[string]bool {
	return recipientsFor(items, "")
}

func recipientsFor(items []Notification, chamber string) map[string]bool {
	result := map[string]bool{}
	for _, item := range items {
		if chamber != "" && item.ChamberID != chamber {
			continue
		}
		result[item.RecipientID] = true
	}
	return result
}

func TestIngestIsIdempotentWithDeterministicIDs(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service, _ := newTestService(t, store, nil)
	event := makeEvent(t, "chamber-1", model.EventEmergencyStarted)

	for range 3 {
		if err := service.Ingest(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	all, _ := store.ListNotifications(ctx)
	if len(all) != 3 {
		t.Fatalf("expected exactly one notification per recipient, got %d", len(all))
	}
	want := deterministicID(event.ID, "crit")
	got, ok, err := store.Notification(ctx, want)
	if err != nil || !ok {
		t.Fatalf("deterministic notification id not found: %v err=%v", want, err)
	}
	if got.SourceEventID != event.ID {
		t.Errorf("source event id mismatch")
	}
}

func TestUnmappedEventsDoNotNotify(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service, _ := newTestService(t, store, nil)
	if err := service.Ingest(ctx, makeEvent(t, "chamber-1", model.EventPlanCreated)); err != nil {
		t.Fatal(err)
	}
	all, _ := store.ListNotifications(ctx)
	if len(all) != 0 {
		t.Fatalf("expected no notifications for plan.created, got %d", len(all))
	}
}

func TestPumpDeliversAndRecordsAttempt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	sender := &fakeSender{}
	service, _ := newTestService(t, store, sender)
	if err := service.Ingest(ctx, makeEvent(t, "chamber-2", model.EventPlanCancelled)); err != nil {
		t.Fatal(err)
	}
	if err := service.Pump(ctx); err != nil {
		t.Fatal(err)
	}
	pending, _ := store.ListPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("expected queue drained, pending=%d", len(pending))
	}
	all, _ := store.ListNotifications(ctx)
	if len(all) != 1 || all[0].Status != StatusSucceeded || all[0].AttemptCount != 1 {
		t.Fatalf("unexpected notification state: %+v", all)
	}
	attempts, _ := store.ListAttempts(ctx, all[0].ID)
	if len(attempts) != 1 || !attempts[0].Succeeded {
		t.Fatalf("expected one successful attempt ledger entry, got %+v", attempts)
	}
	if sender.messages[0].SourceEventID != all[0].SourceEventID.String() {
		t.Errorf("wire message must carry source event id")
	}
}

func TestRetryBackoffThenSuccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	sender := &fakeSender{failLeft: 2, failErr: errors.New("webhook down")}
	service, clock := newTestService(t, store, sender)
	if err := service.Ingest(ctx, makeEvent(t, "chamber-2", model.EventPlanCancelled)); err != nil {
		t.Fatal(err)
	}

	if err := service.Pump(ctx); err != nil { // attempt 1 fails
		t.Fatal(err)
	}
	all, _ := store.ListNotifications(ctx)
	n := all[0]
	if n.Status != StatusPending || n.AttemptCount != 1 {
		t.Fatalf("expected pending after first failure, got %+v", n)
	}
	if !n.NextAttemptAt.Equal(clock.t.Add(time.Second)) {
		t.Errorf("unexpected backoff target %s", n.NextAttemptAt)
	}

	if err := service.Pump(ctx); err != nil { // still within backoff: no send
		t.Fatal(err)
	}
	if sender.count() != 1 {
		t.Fatalf("backoff should prevent immediate retry, sends=%d", sender.count())
	}
	clock.advance(time.Second)
	if err := service.Pump(ctx); err != nil { // attempt 2 fails
		t.Fatal(err)
	}
	clock.advance(2 * time.Second)
	if err := service.Pump(ctx); err != nil { // attempt 3 succeeds
		t.Fatal(err)
	}
	all, _ = store.ListNotifications(ctx)
	if all[0].Status != StatusSucceeded || all[0].AttemptCount != 3 {
		t.Fatalf("expected success on third attempt, got %+v", all[0])
	}
	attempts, _ := store.ListAttempts(ctx, all[0].ID)
	if len(attempts) != 3 || attempts[0].Succeeded || attempts[1].Succeeded || !attempts[2].Succeeded {
		t.Fatalf("attempt ledger does not show failure-then-success: %+v", attempts)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	sender := &fakeSender{failLeft: 1_000_000, failErr: errors.New("permanently down")}
	service, clock := newTestService(t, store, sender)
	service.maxAttempts = 3
	if err := service.Ingest(ctx, makeEvent(t, "chamber-2", model.EventPlanCancelled)); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if err := service.Pump(ctx); err != nil {
			t.Fatal(err)
		}
		clock.advance(time.Hour)
	}
	all, _ := store.ListNotifications(ctx)
	if all[0].Status != StatusDead || all[0].AttemptCount != 3 {
		t.Fatalf("expected dead letter after 3 attempts, got %+v", all[0])
	}
	pending, _ := store.ListPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("dead letters must leave the pending queue")
	}

	// Operator requeues after fixing the endpoint.
	requeued, err := service.ReprocessDead(ctx, all[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if requeued.Status != StatusPending {
		t.Fatalf("expected pending after requeue, got %s", requeued.Status)
	}
	sender.failLeft = 0
	if err := service.Pump(ctx); err != nil {
		t.Fatal(err)
	}
	all, _ = store.ListNotifications(ctx)
	if all[0].Status != StatusSucceeded {
		t.Fatalf("expected requeued notification to deliver, got %s", all[0].Status)
	}
}

func TestRestartKeepsDeterministicStateAndDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sender1 := &fakeSender{failLeft: 1_000_000, failErr: errors.New("down")}
	service1, clock := newTestService(t, store1, sender1)
	event := makeEvent(t, "chamber-1", model.EventEmergencyStarted)
	if err := service1.Ingest(ctx, event); err != nil {
		t.Fatal(err)
	}
	expectedIDs := map[uuid.UUID]bool{}
	all, _ := store1.ListNotifications(ctx)
	for _, n := range all {
		expectedIDs[n.ID] = true
	}
	if err := service1.Pump(ctx); err != nil {
		t.Fatal(err)
	}

	// Simulate process restart: reopen the durable store with fresh memory state.
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sender2 := &fakeSender{}
	service2, _ := newTestService(t, store2, sender2)
	service2.now = clock.now // same clock origin keeps pending items due
	if err := service2.BootstrapFromJournal(ctx, singleEventJournal{event}); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Hour) // clear the pre-crash backoff window
	reloaded, _ := store2.ListNotifications(ctx)
	if len(reloaded) != len(expectedIDs) {
		t.Fatalf("restart duplicated or lost notifications: %d vs %d", len(reloaded), len(expectedIDs))
	}
	for _, n := range reloaded {
		if !expectedIDs[n.ID] {
			t.Errorf("restart produced a different notification id: %s", n.ID)
		}
		if n.AttemptCount != 1 {
			t.Errorf("attempt count should survive restart, got %d", n.AttemptCount)
		}
	}
	if err := service2.Pump(ctx); err != nil {
		t.Fatal(err)
	}
	reloaded, _ = store2.ListNotifications(ctx)
	for _, n := range reloaded {
		if n.Status != StatusSucceeded {
			t.Errorf("notification %s should succeed after recovery, got %s", n.ID, n.Status)
		}
	}
}

type singleEventJournal struct{ event model.Event }

func (j singleEventJournal) Chambers(context.Context) ([]string, error) {
	return []string{j.event.ChamberID}, nil
}
func (j singleEventJournal) Events(_ context.Context, chamber string, _ uint64) ([]model.Event, error) {
	if chamber != j.event.ChamberID {
		return nil, nil
	}
	return []model.Event{j.event}, nil
}

func TestJournalDecoratorIsolatesNotificationFailures(t *testing.T) {
	ctx := context.Background()
	inner := journal.NewMemoryStore()
	decorated := NewJournalStore(inner, nil)
	decorated.BindHook(failingHook{})

	event, err := model.NewEvent("chamber-1", model.EventEmergencyStarted, uuid.Nil, 0, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	persisted, appendErr := decorated.Append(ctx, event)
	if appendErr != nil {
		t.Fatalf("notification failure must not fail the safety append: %v", appendErr)
	}
	if persisted.ID != event.ID {
		t.Fatal("decorator must return the persisted event unchanged")
	}
	events, _ := inner.Events(ctx, "chamber-1", 0)
	if len(events) != 1 {
		t.Fatalf("safety event must be persisted despite hook failure, got %d", len(events))
	}
}

type failingHook struct{}

func (failingHook) Ingest(context.Context, model.Event) error {
	return errors.New("outbox unavailable")
}

func TestBootstrapBackfillsPersistedEvents(t *testing.T) {
	ctx := context.Background()
	journalStore := journal.NewMemoryStore()
	recorder := journal.NewRecorder(journalStore, time.Now)
	for _, kind := range []model.EventType{
		model.EventEmergencyStarted,
		model.EventPlanCancelled,
		model.EventReplayWarning,
		model.EventCycleCreated, // unmapped: must be ignored
	} {
		if _, err := recorder.Record(ctx, "chamber-2", kind, uuid.Nil, 0, map[string]string{"note": "x"}); err != nil {
			t.Fatal(err)
		}
	}
	store := NewMemoryStore()
	service, _ := newTestService(t, store, &fakeSender{})
	if err := service.BootstrapFromJournal(ctx, journalStore); err != nil {
		t.Fatal(err)
	}
	all, _ := store.ListNotifications(ctx)
	// chamber-2: critical→crit+ops (2), notice→ops (1), warning→ops (1) = 4
	if len(all) != 4 {
		t.Fatalf("expected 4 backfilled notifications, got %d", len(all))
	}
	for _, n := range all {
		if n.SourceEventID == uuid.Nil {
			t.Errorf("backfilled notification lost source event id")
		}
	}
}
