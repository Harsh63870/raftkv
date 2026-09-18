// Package cluster handles a fresh node asking an existing cluster to admit it.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/yourname/raftkv/internal/api"
)

const (
	callTimeout = 3 * time.Second
	maxBackoff  = 4 * time.Second
)

type Node struct {
	ID       string
	RaftAddr string
	GRPCAddr string
}

// Join asks each peer in turn to add this node as a voter. Any peer will do:
// non-leaders proxy the request to the leader. Retries until ctx is done.
func Join(ctx context.Context, self Node, peers []string, log *slog.Logger) error {
	if len(peers) == 0 {
		return errors.New("no join peers")
	}

	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		var last error
		for _, peer := range peers {
			if err := request(ctx, peer, self); err != nil {
				last = fmt.Errorf("%s: %w", peer, err)
				continue
			}
			log.Info("joined cluster", "via", peer, "attempt", attempt)
			return nil
		}

		log.Warn("join rejected, retrying", "attempt", attempt, "retry_in", backoff, "err", last)
		select {
		case <-ctx.Done():
			return fmt.Errorf("join: %w (last error: %v)", ctx.Err(), last)
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

func request(ctx context.Context, peer string, self Node) error {
	cc, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	_, err = api.NewKVClient(cc).Join(ctx, &api.JoinRequest{
		NodeId:   self.ID,
		RaftAddr: self.RaftAddr,
		GrpcAddr: self.GRPCAddr,
	})
	return err
}
