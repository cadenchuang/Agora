// Command agora-coordinator accepts client search queries over HTTP, shards
// each query across all alive worker nodes via gRPC scatter-gather, merges the
// global top-K, and tracks worker liveness through pushed heartbeats.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/cadenchuang/agora/internal/logging"
	pb "github.com/cadenchuang/agora/proto/agorapb"
)

type config struct {
	nodeID        string
	grpcAddr      string
	httpAddr      string
	fanoutTimeout time.Duration
	heartbeatTTL  time.Duration
}

func parseConfig() config {
	var c config
	flag.StringVar(&c.nodeID, "node-id", envOr("AGORA_NODE_ID", "coordinator"), "coordinator node id")
	flag.StringVar(&c.grpcAddr, "grpc", envOr("AGORA_GRPC", ":9000"), "gRPC listen address (heartbeat sink)")
	flag.StringVar(&c.httpAddr, "http", envOr("AGORA_HTTP", ":8080"), "HTTP listen address (client API)")
	fanoutMs := envOr("AGORA_FANOUT_MS", "200")
	ttlMs := envOr("AGORA_HEARTBEAT_TTL_MS", "3000")
	flag.Parse()
	c.fanoutTimeout = mustDurationMs(fanoutMs, 200*time.Millisecond)
	c.heartbeatTTL = mustDurationMs(ttlMs, 3*time.Second)
	return c
}

func mustDurationMs(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s + "ms"); err == nil && d > 0 {
		return d
	}
	return def
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := parseConfig()
	log := logging.New("coordinator", cfg.nodeID)

	pool := NewWorkerPool(log, cfg.heartbeatTTL)
	defer pool.Close()
	router := NewRouter(pool, log, cfg.fanoutTimeout)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reaperStop := make(chan struct{})
	go pool.Reap(reaperStop, cfg.heartbeatTTL/2)
	defer close(reaperStop)

	// gRPC server: receives worker heartbeats.
	grpcLis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		log.Error("failed to listen (grpc)", "addr", cfg.grpcAddr, "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterHealthServiceServer(grpcServer, &healthServer{pool: pool, log: log, ttl: cfg.heartbeatTTL})

	// HTTP server: client-facing search API.
	httpServer := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           newHTTPHandler(router, pool, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("coordinator gRPC serving (heartbeat sink)", "addr", cfg.grpcAddr)
		errCh <- grpcServer.Serve(grpcLis)
	}()
	go func() {
		log.Info("coordinator HTTP serving (client API)", "addr", cfg.httpAddr,
			"fanout_timeout_ms", cfg.fanoutTimeout.Milliseconds(), "heartbeat_ttl_ms", cfg.heartbeatTTL.Milliseconds())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received, stopping gracefully")
	case err := <-errCh:
		log.Error("server terminated unexpectedly", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("http graceful shutdown error", "error", err)
	}
	grpcServer.GracefulStop()
}

// healthServer implements the HealthService heartbeat sink.
type healthServer struct {
	pb.UnimplementedHealthServiceServer
	pool *WorkerPool
	log  *slog.Logger
	ttl  time.Duration
}

func (h *healthServer) ReportHeartbeat(_ context.Context, hb *pb.Heartbeat) (*pb.HeartbeatAck, error) {
	if hb.GetNodeId() == "" || hb.GetAddress() == "" {
		return &pb.HeartbeatAck{Accepted: false}, nil
	}
	if err := h.pool.Upsert(hb.GetNodeId(), hb.GetAddress(), hb.GetDocumentCount()); err != nil {
		h.log.Warn("failed to register worker from heartbeat", "worker", hb.GetNodeId(), "error", err)
		return &pb.HeartbeatAck{Accepted: false}, nil
	}
	return &pb.HeartbeatAck{Accepted: true, NextIntervalMs: h.ttl.Milliseconds() / 3}, nil
}
