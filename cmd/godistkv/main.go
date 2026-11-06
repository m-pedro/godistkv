package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m-pedro/godistkv/internal/coordinator"
	"github.com/m-pedro/godistkv/internal/membership"
	"github.com/m-pedro/godistkv/internal/ring"
	"github.com/m-pedro/godistkv/internal/storage"
	"github.com/m-pedro/godistkv/internal/transport"
)

func main() {
	var (
		gatewayAddr = flag.String("gateway", "127.0.0.1:8080", "Public HTTP gateway listen address")
		nodesCount  = flag.Int("nodes", 3, "Number of local storage replica nodes to spawn")
		vnodes      = flag.Int("vnodes", 64, "Virtual nodes per physical storage node")
		nFactor     = flag.Int("n", 3, "Replication factor (N)")
		rQuorum     = flag.Int("r", 2, "Read quorum (R)")
		wQuorum     = flag.Int("w", 2, "Write quorum (W)")
		runFor      = flag.Duration("run-for", 0, "Optional duration before automatic shutdown (0 = run until SIGINT)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *runFor)
		defer cancel()
	}

	hashRing, err := ring.New(*vnodes)
	if err != nil {
		log.Fatalf("failed to create ring: %v", err)
	}

	rpcClient := transport.NewClient(300 * time.Millisecond)
	detector := membership.NewDetector(rpcClient, 2)
	handoff := membership.NewHandoffQueue(rpcClient, detector)

	coord := coordinator.New(coordinator.Config{
		NodeID:  "gateway-coord",
		N:       *nFactor,
		R:       *rQuorum,
		W:       *wQuorum,
		Timeout: 400 * time.Millisecond,
	}, hashRing, rpcClient)
	coord.SetHintSink(handoff)

	// Start N local replica HTTP servers on ephemeral localhost ports
	var replicaServers []*http.Server
	for i := 1; i <= *nodesCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		store := storage.NewEngine()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatalf("failed to listen for %s: %v", nodeID, err)
		}
		baseURL := "http://" + ln.Addr().String()
		srv := &http.Server{Handler: transport.NewReplicaHandler(nodeID, store)}
		go func(s *http.Server, l net.Listener) {
			_ = s.Serve(l)
		}(srv, ln)
		replicaServers = append(replicaServers, srv)

		coord.RegisterPeer(nodeID, baseURL)
		detector.RegisterPeer(nodeID, baseURL)
		log.Printf("[cluster] started replica %s at %s", nodeID, baseURL)
	}

	go detector.Start(ctx, 250*time.Millisecond)
	go handoff.Start(ctx, 250*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key query parameter", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			rec, err := coord.Put(r.Context(), key, body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(rec)

		case http.MethodGet:
			rec, found, err := coord.Get(r.Context(), key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			if !found {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(rec)

		case http.MethodDelete:
			if _, err := coord.Delete(r.Context(), key); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]any{
			"nodes":                  hashRing.Nodes(),
			"pending_hints":          handoff.PendingCount(),
			"replayed_hints":         handoff.ReplayedCount(),
			"read_repairs_completed": coord.ReadRepairsCompleted(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	gwServer := &http.Server{
		Addr:    *gatewayAddr,
		Handler: mux,
	}
	go func() {
		log.Printf("[gateway] listening on http://%s (N=%d, R=%d, W=%d)", *gatewayAddr, *nFactor, *rQuorum, *wQuorum)
		if err := gwServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[shutdown] draining gateway and %d replica servers...", len(replicaServers))
	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = gwServer.Shutdown(shutCtx)
	for _, s := range replicaServers {
		_ = s.Shutdown(shutCtx)
	}
}
