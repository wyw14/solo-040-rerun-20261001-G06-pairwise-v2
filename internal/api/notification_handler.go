package api

import (
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/notify"
)

// notificationsHandler returns the full outbox ledger. Operators use it as the
// delivery-records view; ?status=pending narrows it to outstanding work.
func notificationsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("status") {
		case "pending":
			items, err := runtime.Notify.ListPending(r.Context())
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, items)
		case "", "all":
			items, err := runtime.Notify.ListAll(r.Context())
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, items)
		default:
			writeError(w, http.StatusBadRequest, errors.New("status must be pending or all"))
		}
	}
}

// notificationAttemptsHandler exposes the delivery attempt ledger for one
// notification, including every failed retry and its error.
func notificationAttemptsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chiURLParam(r, "notificationID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		notification, ok, err := runtime.Notify.NotificationView(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("notification not found"))
			return
		}
		attempts, err := runtime.Notify.Attempts(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"notification": notification, "attempts": attempts,
		})
	}
}

// reprocessNotificationHandler returns a dead-lettered notification to the
// pending queue after an operator has fixed the underlying endpoint.
func reprocessNotificationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chiURLParam(r, "notificationID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		notification, err := runtime.Notify.ReprocessDead(r.Context(), id)
		if err != nil {
			if errors.Is(err, notify.ErrNotificationNotFound) {
				writeError(w, http.StatusNotFound, err)
				return
			}
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, notification)
	}
}
