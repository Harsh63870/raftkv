package api

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/yourname/raftkv/internal/store"
)

// Set when a node proxies a write, so the leader never bounces it back.
const forwardedMD = "raftkv-forwarded"

const forwardTimeout = 5 * time.Second

type Server struct {
	UnimplementedKVServer

	store *store.Store
	log   *slog.Logger
	grpc  *grpc.Server

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func New(st *store.Store, log *slog.Logger) *Server {
	s := &Server{
		store: st,
		log:   log,
		grpc:  grpc.NewServer(),
		conns: map[string]*grpc.ClientConn{},
	}
	RegisterKVServer(s.grpc, s)
	return s
}

func (s *Server) Serve(lis net.Listener) error { return s.grpc.Serve(lis) }

func (s *Server) Stop() {
	s.grpc.GracefulStop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cc := range s.conns {
		cc.Close()
	}
}

func (s *Server) Get(_ context.Context, req *GetRequest) (*GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key required")
	}
	// Served from the local FSM: fast, but may trail the leader.
	v, ok := s.store.Get(req.Key)
	return &GetResponse{Value: v, Found: ok, ServedBy: s.store.ID()}, nil
}

func (s *Server) Set(ctx context.Context, req *SetRequest) (*SetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key required")
	}
	if s.store.IsLeader() {
		return &SetResponse{}, applyError(s.store.Set(req.Key, req.Value))
	}

	leader, client, err := s.leader(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := outgoing(ctx)
	defer cancel()
	if _, err := client.Set(ctx, req); err != nil {
		return nil, err
	}
	return &SetResponse{ForwardedTo: leader}, nil
}

func (s *Server) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key required")
	}
	if s.store.IsLeader() {
		return &DeleteResponse{}, applyError(s.store.Delete(req.Key))
	}

	leader, client, err := s.leader(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := outgoing(ctx)
	defer cancel()
	if _, err := client.Delete(ctx, req); err != nil {
		return nil, err
	}
	return &DeleteResponse{ForwardedTo: leader}, nil
}

func (s *Server) Join(ctx context.Context, req *JoinRequest) (*JoinResponse, error) {
	if req.NodeId == "" || req.RaftAddr == "" || req.GrpcAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id, raft_addr and grpc_addr required")
	}
	if s.store.IsLeader() {
		return &JoinResponse{}, applyError(s.store.Join(req.NodeId, req.RaftAddr, req.GrpcAddr))
	}

	_, client, err := s.leader(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := outgoing(ctx)
	defer cancel()
	return client.Join(ctx, req)
}

func (s *Server) Snapshot(_ context.Context, _ *SnapshotRequest) (*SnapshotResponse, error) {
	idx, err := s.store.Snapshot()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "snapshot: %v", err)
	}
	return &SnapshotResponse{Index: idx}, nil
}

func (s *Server) Status(_ context.Context, _ *StatusRequest) (*StatusResponse, error) {
	servers, err := s.store.Servers()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read configuration: %v", err)
	}

	stats := s.store.Stats()
	leader := s.store.Leader()
	addrs := s.store.FSM().Peers()

	peers := make([]*Peer, 0, len(servers))
	for _, srv := range servers {
		peers = append(peers, &Peer{
			NodeId:   string(srv.ID),
			RaftAddr: string(srv.Address),
			GrpcAddr: addrs[string(srv.ID)],
			Leader:   string(srv.ID) == leader.ID,
		})
	}

	return &StatusResponse{
		NodeId:       s.store.ID(),
		State:        stats["state"],
		LeaderId:     leader.ID,
		LeaderAddr:   leader.GRPCAddr,
		Term:         parseUint(stats["term"]),
		AppliedIndex: parseUint(stats["applied_index"]),
		LastIndex:    parseUint(stats["last_log_index"]),
		Peers:        peers,
		Keys:         int32(s.store.FSM().Len()),
	}, nil
}

// leader returns a client for the current leader, refusing to forward a
// request that was already forwarded to us.
func (s *Server) leader(ctx context.Context) (string, KVClient, error) {
	l := s.store.Leader()
	switch {
	case l.ID == "":
		return "", nil, status.Error(codes.Unavailable, "no leader elected")
	case l.GRPCAddr == "":
		return "", nil, status.Errorf(codes.Unavailable,
			"not leader, leader is at %s (client address not yet replicated)", l.RaftAddr)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(forwardedMD)) > 0 {
		return "", nil, status.Errorf(codes.FailedPrecondition, "not leader, leader is at %s", l.GRPCAddr)
	}

	cc, err := s.conn(l.GRPCAddr)
	if err != nil {
		return "", nil, err
	}
	return l.GRPCAddr, NewKVClient(cc), nil
}

func (s *Server) conn(addr string) (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cc, ok := s.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial leader %s: %v", addr, err)
	}
	s.conns[addr] = cc
	return cc, nil
}

func outgoing(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = metadata.AppendToOutgoingContext(ctx, forwardedMD, "1")
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, forwardTimeout)
}

func parseUint(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func applyError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, "lost leadership mid-write, retry")
	default:
		return status.Errorf(codes.Internal, "apply: %v", err)
	}
}
