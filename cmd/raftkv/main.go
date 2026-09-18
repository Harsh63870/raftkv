package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yourname/raftkv/internal/api"
	"github.com/yourname/raftkv/internal/cluster"
	"github.com/yourname/raftkv/internal/store"
)

func main() {
	var (
		id           = flag.String("id", "node1", "node id, unique in the cluster")
		raftBind     = flag.String("raft-addr", "127.0.0.1:9001", "raft bind address")
		raftAdvtz    = flag.String("raft-advertise", "", "raft address peers dial (default -raft-addr)")
		grpcBind     = flag.String("grpc-addr", "127.0.0.1:50051", "gRPC bind address")
		grpcAdvtz    = flag.String("grpc-advertise", "", "gRPC address peers dial (default -grpc-addr)")
		join         = flag.String("join", "", "comma-separated gRPC addresses of existing nodes")
		dataDir      = flag.String("data-dir", "data", "directory for raft log and snapshots")
		bootstrap    = flag.Bool("bootstrap", false, "bootstrap a new single-node cluster")
		logLevel     = flag.String("log-level", "info", "debug|info|warn|error")
		raftLogLevel = flag.String("raft-log-level", "warn", "log level for raft internals")
	)
	flag.Parse()

	var level slog.LevelVar
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		exit("invalid -log-level: %v", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: &level})).With("node", *id)

	raftAddr := orDefault(*raftAdvtz, *raftBind)
	grpcAddr := orDefault(*grpcAdvtz, *grpcBind)
	peers := split(*join)

	if *bootstrap && len(peers) > 0 {
		exit("-bootstrap and -join are mutually exclusive")
	}
	if !*bootstrap && len(peers) == 0 {
		exit("need -bootstrap or -join")
	}

	st, err := store.Open(store.Config{
		NodeID:    *id,
		RaftBind:  *raftBind,
		RaftAdvtz: raftAddr,
		GRPCAddr:  grpcAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
		LogLevel:  *raftLogLevel,
	}, log)
	if err != nil {
		exit("open store: %v", err)
	}

	lis, err := net.Listen("tcp", *grpcBind)
	if err != nil {
		exit("listen %s: %v", *grpcBind, err)
	}

	srv := api.New(st, log)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Info("serving", "grpc", grpcAddr, "raft", raftAddr, "data_dir", *dataDir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(peers) > 0 {
		go func() {
			if err := cluster.Join(ctx, cluster.Node{ID: *id, RaftAddr: raftAddr, GRPCAddr: grpcAddr}, peers, log); err != nil {
				log.Error("join cluster", "err", err)
			}
		}()
	}

	select {
	case err := <-serveErr:
		log.Error("grpc server stopped", "err", err)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	srv.Stop()
	if err := st.Close(); err != nil {
		log.Error("close store", "err", err)
	}
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func split(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func exit(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
