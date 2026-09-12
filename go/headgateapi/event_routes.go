package headgateapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

const eventStreamWriteTimeout = 30 * time.Second

func refreshEventStreamWriteDeadline(controller *http.ResponseController) error {
	err := controller.SetWriteDeadline(time.Now().Add(eventStreamWriteTimeout))
	if errors.Is(err, http.ErrNotSupported) {
		return nil
	}
	return err
}

func (a *api) events(w http.ResponseWriter, r *http.Request) {
	_, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	controller := http.NewResponseController(w)
	// http.Server.WriteTimeout is an absolute response deadline. Refresh it for
	// each SSE write so a healthy long-lived stream is not cut off after one
	// server timeout while a stalled client still has a bounded write.
	if err := refreshEventStreamWriteDeadline(controller); err != nil {
		errJSON(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.WriteHeader(200)
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
			if err := refreshEventStreamWriteDeadline(controller); err != nil {
				return
			}
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
			if err := refreshEventStreamWriteDeadline(controller); err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: queue_activity\ndata: %s\n\n", data); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
	}
}
