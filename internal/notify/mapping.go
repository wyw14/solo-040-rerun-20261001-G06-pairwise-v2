package notify

import (
	"github.com/wyw14/cry-96/internal/model"
)

// classify maps domain events to notification severity and an operator-facing
// summary. It deliberately mirrors the incident review meanings so the two
// views never disagree about an event. Events not returned here never leave
// the safety journal.
func classify(event model.Event) (Severity, string, bool) {
	switch event.Type {
	case model.EventEmergencyStarted:
		return SeverityCritical, "Emergency stop requested", true
	case model.EventEmergencySettled:
		return SeverityNotice, "Emergency equipment reached stable state", true
	case model.EventReplayWarning:
		return SeverityWarning, "Recovery sequence requires review", true
	case model.EventPlanCancelled:
		return SeverityNotice, "Convoy plan cancelled and compensated", true
	default:
		return "", "", false
	}
}
