package main

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/cadenchuang/agora/proto/agorapb"
)

// Search executes a local BM25 query against this worker's shard and returns
// its local top-k hits, stamped with this node's id.
func (s *workerServer) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	start := time.Now()

	// Honor client cancellation/deadline before doing any work.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	filters := make(map[string]string, len(req.GetFilters()))
	for _, f := range req.GetFilters() {
		filters[f.GetKey()] = f.GetValue()
	}

	topK := int(req.GetTopK())
	if topK <= 0 {
		topK = 10
	}

	hits := s.idx.Search(req.GetQuery(), topK, filters)
	results := make([]*pb.SearchResult, 0, len(hits))
	for _, h := range hits {
		results = append(results, &pb.SearchResult{
			DocId:       h.DocID,
			Score:       h.Score,
			TextSnippet: h.Snippet,
			NodeId:      s.nodeID,
		})
	}

	elapsed := time.Since(start).Milliseconds()
	s.log.Debug("local search complete",
		"request_id", req.GetRequestId(),
		"query", req.GetQuery(),
		"hits", len(results),
		"elapsed_ms", elapsed,
	)

	return &pb.SearchResponse{
		Results:         results,
		ExecutionTimeMs: elapsed,
		ServedBy:        []string{s.nodeID},
	}, nil
}

// Check answers a pull-based health probe.
func (s *workerServer) Check(ctx context.Context, _ *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		NodeId:          s.nodeID,
		Active:          true,
		DocumentCount:   int64(s.idx.DocCount()),
		TimestampUnixMs: time.Now().UnixMilli(),
	}, nil
}

// runHeartbeat dials the coordinator and pushes a Heartbeat on a fixed interval
// until ctx is cancelled. The connection is lazily (re)established so a worker
// can start before the coordinator is up and recover if it restarts.
func (s *workerServer) runHeartbeat(ctx context.Context, coordinatorAddr, advertiseAddr string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var conn *grpc.ClientConn
	var client pb.HealthServiceClient
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	ensureConn := func() error {
		if conn != nil {
			return nil
		}
		c, err := grpc.NewClient(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		conn = c
		client = pb.NewHealthServiceClient(c)
		return nil
	}

	send := func() {
		if err := ensureConn(); err != nil {
			s.log.Warn("heartbeat: cannot connect to coordinator", "addr", coordinatorAddr, "error", err)
			return
		}
		hbCtx, cancel := context.WithTimeout(ctx, interval)
		defer cancel()
		ack, err := client.ReportHeartbeat(hbCtx, &pb.Heartbeat{
			NodeId:          s.nodeID,
			Address:         advertiseAddr,
			Active:          true,
			DocumentCount:   int64(s.idx.DocCount()),
			TimestampUnixMs: time.Now().UnixMilli(),
		})
		if err != nil {
			s.log.Warn("heartbeat failed", "error", err)
			// Drop the connection so the next tick redials.
			if conn != nil {
				_ = conn.Close()
				conn = nil
			}
			return
		}
		s.log.Debug("heartbeat acked", "next_interval_ms", ack.GetNextIntervalMs())
	}

	// Send one immediately so the coordinator learns about us without waiting.
	send()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}
