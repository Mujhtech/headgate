package headgateapi

import (
	"net/http"

	headgate "github.com/mujhtech/headgate/go"
)

func operationJSON(op headgate.OperationStatus) map[string]any {
	var e any
	if op.Error != "" {
		e = op.Error
	}
	return map[string]any{
		"id": op.ID, "status": op.Status, "affected": op.Affected,
		"total_estimated": op.TotalEstimated, "dry_run": op.DryRun, "error": e,
	}
}

func (a *api) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := a.store.GetOperation(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if op == nil {
		errJSON(w, http.StatusNotFound, "no such operation")
		return
	}
	writeJSON(w, 200, operationJSON(*op))
}
