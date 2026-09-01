package headgateshared

import "fmt"

var (
	retryStates  = []string{"archived"}
	cancelStates = []string{"scheduled", "available", "running"}
	deleteStates = []string{"scheduled", "available", "retryable", "completed", "archived", "cancelled", "quarantined", "undecodable"}
)

// BulkActionStates returns a private copy of the lifecycle states an action may touch.
func BulkActionStates(action string) ([]string, bool) {
	var states []string
	switch action {
	case "retry":
		states = retryStates
	case "cancel":
		states = cancelStates
	case "delete":
		states = deleteStates
	default:
		return nil, false
	}
	return append([]string(nil), states...), true
}

func ValidWorkerCommand(command string) bool {
	switch command {
	case "", "quiet", "resume", "restart", "terminate", "resign":
		return true
	default:
		return false
	}
}

func FormatGeneratedID(nowMs int64, processID int, sequence uint64) string {
	return fmt.Sprintf("hg%012x%05x%04x", nowMs, processID&0xfffff, sequence&0xffff)
}
