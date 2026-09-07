package headgateapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func (a *api) events(w http.ResponseWriter, r *http.Request) {
	_, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		return
	}
	ns, notifying := any(a.store).(headgate.NotifyingStore)
	notifying = notifying && a.store.Caps().Has(headgate.CapNotifying)
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	wake := make(chan string, 16)
	if notifying {
		go func() {
			for {
				q, ok, err := ns.WaitWakeup(r.Context(), nil, time.Hour)
				if err != nil || r.Context().Err() != nil {
					return
				}
				if ok {
					select {
					case wake <- q:
					case <-r.Context().Done():
						return
					}
				}
			}
		}()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": hb\n\n"); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		case first := <-wake:
			queues := map[string]struct{}{}
			if first != "" {
				queues[first] = struct{}{}
			}
			deadline := time.NewTimer(200 * time.Millisecond)
		coalesce:
			for {
				select {
				case <-r.Context().Done():
					if !deadline.Stop() {
						<-deadline.C
					}
					return
				case q := <-wake:
					if q != "" {
						queues[q] = struct{}{}
					}
				case <-deadline.C:
					break coalesce
				}
			}
			names := make([]string, 0, len(queues))
			for q := range queues {
				names = append(names, q)
			}
			data, _ := json.Marshal(map[string]any{"queues": names})
			if _, err := fmt.Fprintf(w, "event: queue_activity\ndata: %s\n\n", data); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
	}
}
