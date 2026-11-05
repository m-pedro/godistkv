package transport

import (
	"encoding/json"
	"net/http"

	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/version"
)

// ReplicaHandler exposes internal peer-to-peer RPC endpoints for a storage node.
type ReplicaHandler struct {
	NodeID string
	Store  *storage.Engine
	mux    *http.ServeMux
}

func NewReplicaHandler(nodeID string, store *storage.Engine) *ReplicaHandler {
	h := &ReplicaHandler{
		NodeID: nodeID,
		Store:  store,
		mux:    http.NewServeMux(),
	}
	h.mux.HandleFunc("/internal/kv", h.handleKV)
	h.mux.HandleFunc("/internal/ping", h.handlePing)
	return h
}

func (h *ReplicaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *ReplicaHandler) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Node-ID", h.NodeID)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("PONG"))
}

func (h *ReplicaHandler) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		var incoming version.VersionedRecord
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			http.Error(w, "invalid record payload", http.StatusBadRequest)
			return
		}
		winner, _ := h.Store.Put(incoming)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(winner)

	case http.MethodGet:
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key parameter", http.StatusBadRequest)
			return
		}
		rec, ok := h.Store.Get(key)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(rec)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
