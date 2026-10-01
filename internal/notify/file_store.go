package notify

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// FileStore keeps two append-only JSONL files under <root>/notify.
// notifications.jsonl holds the latest state of each notification (last record
// wins on replay); attempts.jsonl is the immutable delivery ledger. Both files
// are fsynced per write, so a process restart never loses or duplicates work.
type FileStore struct {
	root string
	mu   sync.Mutex
	mem  *index
}

func NewFileStore(root string) (*FileStore, error) {
	dir := filepath.Join(root, "notify")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	store := &FileStore{root: dir, mem: newIndex()}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileStore) notificationsPath() string { return filepath.Join(s.root, "notifications.jsonl") }
func (s *FileStore) attemptsPath() string      { return filepath.Join(s.root, "attempts.jsonl") }

func (s *FileStore) load() error {
	notifications, err := readJSONL[Notification](s.notificationsPath())
	if err != nil {
		return err
	}
	for _, notification := range notifications {
		s.mem.apply(notification)
	}
	attempts, err := readJSONL[Attempt](s.attemptsPath())
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		s.mem.attempts[attempt.NotificationID] = append(s.mem.attempts[attempt.NotificationID], attempt)
	}
	return nil
}

func readJSONL[T any](path string) ([]T, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var result []T
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var value T
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, scanner.Err()
}

func appendJSONL(path string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(encoded, '\n')); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func (s *FileStore) SaveNotification(_ context.Context, notification Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := appendJSONL(s.notificationsPath(), notification); err != nil {
		return err
	}
	s.mem.apply(notification)
	return nil
}

func (s *FileStore) AppendAttempt(_ context.Context, attempt Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := appendJSONL(s.attemptsPath(), attempt); err != nil {
		return err
	}
	s.mem.attempts[attempt.NotificationID] = append(s.mem.attempts[attempt.NotificationID], attempt)
	return nil
}

func (s *FileStore) Notification(_ context.Context, id uuid.UUID) (Notification, bool, error) {
	s.mu.Lock()
	notification, ok := s.mem.notifications[id]
	s.mu.Unlock()
	return notification, ok, nil
}

func (s *FileStore) HasAddress(_ context.Context, sourceEventID uuid.UUID, recipientID string) (bool, error) {
	s.mu.Lock()
	_, ok := s.mem.addresses[[2]string{sourceEventID.String(), recipientID}]
	s.mu.Unlock()
	return ok, nil
}

func (s *FileStore) ListNotifications(_ context.Context) ([]Notification, error) {
	s.mu.Lock()
	result := s.mem.list()
	s.mu.Unlock()
	return result, nil
}

func (s *FileStore) ListPending(_ context.Context) ([]Notification, error) {
	s.mu.Lock()
	result := s.mem.pending()
	s.mu.Unlock()
	return result, nil
}

func (s *FileStore) ListAttempts(_ context.Context, notificationID uuid.UUID) ([]Attempt, error) {
	s.mu.Lock()
	result := s.mem.attemptsFor(notificationID)
	s.mu.Unlock()
	return result, nil
}
