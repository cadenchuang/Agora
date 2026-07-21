package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/cadenchuang/agora/proto/agorapb"
)

// Router performs the scatter-gather query: fan out to all alive workers
// concurrently, enforce a per-fan-out deadline, and merge surviving results.
type Router struct {
	pool          *WorkerPool
	log           *slog.Logger
	fanoutTimeout time.Duration
}

func NewRouter(pool *WorkerPool, log *slog.Logger, fanoutTimeout time.Duration) *Router {
	return &Router{pool: pool, log: log, fanoutTimeout: fanoutTimeout}
}

// workerReply bundles one worker's outcome for aggregation.
type workerReply struct {
	nodeID  string
	results []*pb.SearchResult
	err     error
}

// Search fans req out to every alive worker, waits up to fanoutTimeout, and
// returns the merged global top-K. Workers that error or exceed the deadline
// are recorded as degraded but do not fail the overall request: the coordinator
// returns whatever the survivors produced (graceful partial results).
func (r *Router) Search(ctx context.Context, req *pb.SearchRequest) *pb.SearchResponse {
	start := time.Now()

	workers := r.pool.Alive()
	topK := int(req.GetTopK())
	if topK <= 0 {
		topK = 10
	}

	if len(workers) == 0 {
		r.log.Warn("no alive workers to serve query", "request_id", req.GetRequestId())
		return &pb.SearchResponse{ExecutionTimeMs: time.Since(start).Milliseconds()}
	}

	// Bound the entire fan-out; inherit any tighter caller deadline.
	fanCtx, cancel := context.WithTimeout(ctx, r.fanoutTimeout)
	defer cancel()

	replies := make(chan workerReply, len(workers))
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *worker) {
			defer wg.Done()
			// Each worker gets the same top_k; local pruning keeps payloads small.
			resp, err := w.client.Search(fanCtx, req)
			if err != nil {
				replies <- workerReply{nodeID: w.nodeID, err: err}
				return
			}
			replies <- workerReply{nodeID: w.nodeID, results: resp.GetResults()}
		}(w)
	}

	// Close the channel once all fan-out goroutines report in.
	go func() {
		wg.Wait()
		close(replies)
	}()

	partials := make([][]*pb.SearchResult, 0, len(workers))
	servedBy := make([]string, 0, len(workers))
	degraded := make([]string, 0)

	for reply := range replies {
		if reply.err != nil {
			degraded = append(degraded, reply.nodeID)
			r.log.Warn("worker failed or timed out during fan-out",
				"request_id", req.GetRequestId(),
				"worker", reply.nodeID,
				"error", reply.err,
			)
			continue
		}
		servedBy = append(servedBy, reply.nodeID)
		partials = append(partials, reply.results)
	}

	merged := mergeTopK(partials, topK)
	elapsed := time.Since(start).Milliseconds()

	r.log.Info("scatter-gather complete",
		"request_id", req.GetRequestId(),
		"query", req.GetQuery(),
		"fanned_out", len(workers),
		"served_by", len(servedBy),
		"degraded", len(degraded),
		"results", len(merged),
		"elapsed_ms", elapsed,
	)

	return &pb.SearchResponse{
		Results:         merged,
		ExecutionTimeMs: elapsed,
		ServedBy:        servedBy,
		DegradedNodes:   degraded,
	}
}
