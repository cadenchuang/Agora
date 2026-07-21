package main

import (
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/cadenchuang/agora/proto/agorapb"
)

// worker is the coordinator's view of a single edge worker node, including its
// reusable gRPC connection and last-seen liveness data.
type worker struct {
	nodeID   string
	address  string
	conn     *grpc.ClientConn
	client   pb.SearchServiceClient
	docCount int64
	lastSeen time.Time
}

// WorkerPool is a thread-safe registry of live workers. Workers are (de)registered
// from heartbeats; a background reaper evicts nodes that miss their TTL.
type WorkerPool struct {
	log *slog.Logger
	ttl time.Duration

	mu      sync.RWMutex
	workers map[string]*worker // keyed by node id.
}

// NewWorkerPool creates a pool that considers a worker dead after ttl elapses
// without a heartbeat.
func NewWorkerPool(log *slog.Logger, ttl time.Duration) *WorkerPool {
	return &WorkerPool{
		log:     log,
		ttl:     ttl,
		workers: make(map[string]*worker),
	}
}

// Upsert registers or refreshes a worker from a heartbeat. If the node is new,
// or its advertised address changed, a fresh gRPC connection is established and
// any stale connection is closed.
func (p *WorkerPool) Upsert(nodeID, address string, docCount int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	w, exists := p.workers[nodeID]
	if exists && w.address == address {
		w.docCount = docCount
		w.lastSeen = time.Now()
		return nil
	}

	// New node or changed address: (re)dial.
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	if exists && w.conn != nil {
		_ = w.conn.Close()
	}

	p.workers[nodeID] = &worker{
		nodeID:   nodeID,
		address:  address,
		conn:     conn,
		client:   pb.NewSearchServiceClient(conn),
		docCount: docCount,
		lastSeen: time.Now(),
	}
	if !exists {
		p.log.Info("worker registered", "worker", nodeID, "address", address, "documents", docCount)
	} else {
		p.log.Info("worker address updated", "worker", nodeID, "address", address)
	}
	return nil
}

// Alive returns a snapshot of workers seen within the TTL. The returned slice is
// safe for the caller to iterate without holding the lock.
func (p *WorkerPool) Alive() []*worker {
	cutoff := time.Now().Add(-p.ttl)
	p.mu.RLock()
	defer p.mu.RUnlock()

	alive := make([]*worker, 0, len(p.workers))
	for _, w := range p.workers {
		if w.lastSeen.After(cutoff) && w.conn.GetState() != connectivity.Shutdown {
			alive = append(alive, w)
		}
	}
	return alive
}

// Reap runs until ctx-driven stop, evicting and closing workers past their TTL.
// It is intended to be launched as a goroutine.
func (p *WorkerPool) Reap(stop <-chan struct{}, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p.evictExpired()
		}
	}
}

func (p *WorkerPool) evictExpired() {
	cutoff := time.Now().Add(-p.ttl)
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, w := range p.workers {
		if w.lastSeen.Before(cutoff) {
			p.log.Warn("worker evicted (heartbeat TTL exceeded)", "worker", id, "last_seen", w.lastSeen.Format(time.RFC3339))
			if w.conn != nil {
				_ = w.conn.Close()
			}
			delete(p.workers, id)
		}
	}
}

// Close tears down all worker connections.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.workers {
		if w.conn != nil {
			_ = w.conn.Close()
		}
	}
	p.workers = make(map[string]*worker)
}
