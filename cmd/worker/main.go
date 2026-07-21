// Command agora-worker holds an in-memory inverted index for one document
// partition, serves local BM25 search over gRPC, answers health checks, and
// pushes periodic heartbeats to the coordinator.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/cadenchuang/agora/internal/logging"
	"github.com/cadenchuang/agora/pkg/index"
	pb "github.com/cadenchuang/agora/proto/agorapb"
)

type config struct {
	nodeID            string
	listenAddr        string
	advertiseAddr     string
	shardPath         string
	coordinatorAddr   string
	heartbeatInterval time.Duration
}

func parseConfig() config {
	var c config
	flag.StringVar(&c.nodeID, "node-id", envOr("AGORA_NODE_ID", "worker-0"), "unique node identifier")
	flag.StringVar(&c.listenAddr, "listen", envOr("AGORA_LISTEN", ":9101"), "gRPC listen address")
	flag.StringVar(&c.advertiseAddr, "advertise", envOr("AGORA_ADVERTISE", ""), "address advertised to coordinator (defaults to listen)")
	flag.StringVar(&c.shardPath, "shard", envOr("AGORA_SHARD", "data/shard_0.json"), "path to shard JSON file")
	flag.StringVar(&c.coordinatorAddr, "coordinator", envOr("AGORA_COORDINATOR", ""), "coordinator gRPC address for heartbeats (empty disables)")
	interval := envOr("AGORA_HEARTBEAT_MS", "1000")
	flag.Parse()
	ms, err := time.ParseDuration(interval + "ms")
	if err != nil || ms <= 0 {
		ms = time.Second
	}
	c.heartbeatInterval = ms
	if c.advertiseAddr == "" {
		c.advertiseAddr = c.listenAddr
	}
	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := parseConfig()
	log := logging.New("worker", cfg.nodeID)

	idx := index.New()
	n, err := index.LoadShardFile(idx, cfg.shardPath)
	if err != nil {
		log.Error("failed to load shard", "path", cfg.shardPath, "error", err)
		os.Exit(1)
	}
	log.Info("shard loaded", "path", cfg.shardPath, "documents", n)

	srv := &workerServer{
		nodeID: cfg.nodeID,
		idx:    idx,
		log:    log,
	}

	lis, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		log.Error("failed to listen", "addr", cfg.listenAddr, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSearchServiceServer(grpcServer, srv)
	pb.RegisterHealthServiceServer(grpcServer, srv)

	// Root context cancelled on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.coordinatorAddr != "" {
		go srv.runHeartbeat(ctx, cfg.coordinatorAddr, cfg.advertiseAddr, cfg.heartbeatInterval)
	} else {
		log.Warn("heartbeats disabled: no coordinator address configured")
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("worker serving", "listen", cfg.listenAddr, "advertise", cfg.advertiseAddr)
		serveErr <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received, stopping gracefully")
		grpcServer.GracefulStop()
	case err := <-serveErr:
		if err != nil {
			log.Error("grpc server terminated", "error", err)
			os.Exit(1)
		}
	}
}

// workerServer implements both SearchServiceServer and HealthServiceServer.
type workerServer struct {
	pb.UnimplementedSearchServiceServer
	pb.UnimplementedHealthServiceServer

	nodeID string
	idx    *index.InvertedIndex
	log    *slog.Logger
}
