// Package store owns the raft node: transport, log/stable/snapshot stores,
// cluster membership and the write path into the FSM.
package store

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/yourname/raftkv/internal/fsm"
)

var ErrNotLeader = errors.New("not leader")

const (
	retainSnapshots = 2
	applyTimeout    = 5 * time.Second
	membershipWait  = 5 * time.Second
)

type Config struct {
	NodeID    string
	RaftBind  string
	RaftAdvtz string
	GRPCAddr  string
	DataDir   string
	Bootstrap bool
	LogLevel  string
}

type Store struct {
	cfg  Config
	log  *slog.Logger
	fsm  *fsm.FSM
	raft *raft.Raft

	trans *raft.NetworkTransport
	bolt  *boltdb.BoltStore
}

type Leader struct {
	ID       string
	RaftAddr string
	GRPCAddr string
}

func Open(cfg Config, log *slog.Logger) (*Store, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("node id required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, err
	}

	advertise, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvtz)
	if err != nil {
		return nil, fmt.Errorf("resolve advertise addr %q: %w", cfg.RaftAdvtz, err)
	}
	if advertise.IP == nil || advertise.IP.IsUnspecified() {
		return nil, fmt.Errorf("advertise addr %q must be routable by peers", cfg.RaftAdvtz)
	}

	// raft's internals log through hclog; keep it at WARN and JSON so it
	// interleaves cleanly with our slog output instead of drowning it.
	hl := hclog.New(&hclog.LoggerOptions{
		Name:       "raft",
		Level:      hclog.LevelFromString(cfg.LogLevel),
		Output:     os.Stderr,
		JSONFormat: true,
	})

	trans, err := raft.NewTCPTransportWithLogger(cfg.RaftBind, advertise, 3, 10*time.Second, hl)
	if err != nil {
		return nil, fmt.Errorf("tcp transport on %q: %w", cfg.RaftBind, err)
	}

	snaps, err := raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, retainSnapshots, hl)
	if err != nil {
		trans.Close()
		return nil, err
	}

	bolt, err := boltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		trans.Close()
		return nil, err
	}
	logs, err := raft.NewLogCache(512, bolt)
	if err != nil {
		bolt.Close()
		trans.Close()
		return nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.Logger = hl
	// Tightened from the defaults so failover completes in ~1s for the demo.
	rc.HeartbeatTimeout = 500 * time.Millisecond
	rc.ElectionTimeout = 500 * time.Millisecond
	rc.LeaderLeaseTimeout = 250 * time.Millisecond
	rc.CommitTimeout = 50 * time.Millisecond
	rc.SnapshotThreshold = 2048
	rc.SnapshotInterval = 30 * time.Second

	s := &Store{cfg: cfg, log: log, fsm: fsm.New(log), trans: trans, bolt: bolt}

	hasState, err := raft.HasExistingState(logs, bolt, snaps)
	if err != nil {
		bolt.Close()
		trans.Close()
		return nil, err
	}

	r, err := raft.NewRaft(rc, s.fsm, logs, bolt, snaps, trans)
	if err != nil {
		bolt.Close()
		trans.Close()
		return nil, err
	}
	s.raft = r

	switch {
	case hasState:
		log.Info("recovered existing raft state", "node_id", cfg.NodeID, "data_dir", cfg.DataDir)
	case cfg.Bootstrap:
		f := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			ID:      rc.LocalID,
			Address: trans.LocalAddr(),
		}}})
		if err := f.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
		log.Info("bootstrapped cluster", "node_id", cfg.NodeID, "raft_addr", trans.LocalAddr())
	default:
		log.Info("waiting to be added to a cluster", "node_id", cfg.NodeID)
	}

	obs := make(chan raft.Observation, 32)
	r.RegisterObserver(raft.NewObserver(obs, false, nil))
	go s.observe(obs)
	go s.watchLeadership()

	return s, nil
}

func (s *Store) FSM() *fsm.FSM  { return s.fsm }
func (s *Store) ID() string     { return s.cfg.NodeID }
func (s *Store) IsLeader() bool { return s.raft.State() == raft.Leader }

func (s *Store) Get(key string) (string, bool) { return s.fsm.Get(key) }

func (s *Store) Set(key, value string) error {
	return s.apply(fsm.Command{Op: fsm.OpSet, Key: key, Value: value})
}

func (s *Store) Delete(key string) error {
	return s.apply(fsm.Command{Op: fsm.OpDelete, Key: key})
}

// Leader reports the current leader. GRPCAddr is empty until the leader's
// client address has replicated to this node.
func (s *Store) Leader() Leader {
	addr, id := s.raft.LeaderWithID()
	if id == "" {
		return Leader{}
	}
	return Leader{ID: string(id), RaftAddr: string(addr), GRPCAddr: s.fsm.PeerAddr(string(id))}
}

// Join adds a voter to the cluster and replicates its client address. Must be
// called on the leader.
func (s *Store) Join(id, raftAddr, grpcAddr string) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}

	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return err
	}

	joined := false
	for _, srv := range cf.Configuration().Servers {
		if srv.ID != raft.ServerID(id) && srv.Address != raft.ServerAddress(raftAddr) {
			continue
		}
		if srv.ID == raft.ServerID(id) && srv.Address == raft.ServerAddress(raftAddr) {
			joined = true
			break
		}
		// Same node under a different id or address: drop the stale entry first.
		s.log.Warn("removing stale cluster member", "node_id", srv.ID, "raft_addr", srv.Address)
		if err := s.raft.RemoveServer(srv.ID, 0, membershipWait).Error(); err != nil {
			return err
		}
	}

	if !joined {
		if err := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, membershipWait).Error(); err != nil {
			return err
		}
		s.log.Info("node joined", "node_id", id, "raft_addr", raftAddr, "grpc_addr", grpcAddr)
	}

	if s.fsm.PeerAddr(id) == grpcAddr {
		return nil
	}
	return s.apply(fsm.Command{Op: fsm.OpPeer, Key: id, Value: grpcAddr})
}

func (s *Store) Snapshot() (uint64, error) {
	if err := s.raft.Snapshot().Error(); err != nil {
		return 0, err
	}
	idx := s.raft.AppliedIndex()
	s.log.Info("snapshot taken", "index", idx, "keys", s.fsm.Len())
	return idx, nil
}

func (s *Store) Stats() map[string]string { return s.raft.Stats() }

func (s *Store) Servers() ([]raft.Server, error) {
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		return nil, err
	}
	return cf.Configuration().Servers, nil
}

func (s *Store) Close() error {
	if err := s.raft.Shutdown().Error(); err != nil {
		return err
	}
	s.trans.Close()
	return s.bolt.Close()
}

func (s *Store) apply(c fsm.Command) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	b, err := c.Encode()
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, applyTimeout)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return ErrNotLeader
		}
		return err
	}
	if err, ok := f.Response().(error); ok {
		return err
	}
	return nil
}

// watchLeadership publishes this node's client address on every term it wins,
// so followers can resolve it for write forwarding.
func (s *Store) watchLeadership() {
	for isLeader := range s.raft.LeaderCh() {
		if !isLeader {
			s.log.Info("lost leadership", "node_id", s.cfg.NodeID, "term", s.raft.CurrentTerm())
			continue
		}
		s.log.Info("acquired leadership", "node_id", s.cfg.NodeID, "term", s.raft.CurrentTerm())
		if s.fsm.PeerAddr(s.cfg.NodeID) == s.cfg.GRPCAddr {
			continue
		}
		if err := s.apply(fsm.Command{Op: fsm.OpPeer, Key: s.cfg.NodeID, Value: s.cfg.GRPCAddr}); err != nil {
			s.log.Error("publish client address", "node_id", s.cfg.NodeID, "err", err)
		}
	}
}

func (s *Store) observe(ch <-chan raft.Observation) {
	for o := range ch {
		switch v := o.Data.(type) {
		case raft.LeaderObservation:
			if v.LeaderID == "" {
				s.log.Warn("leader lost", "term", s.raft.CurrentTerm())
				continue
			}
			s.log.Info("leader elected",
				"leader_id", v.LeaderID, "leader_addr", v.LeaderAddr, "term", s.raft.CurrentTerm())
		case raft.RaftState:
			s.log.Info("state change", "state", v.String(), "term", s.raft.CurrentTerm())
		case raft.PeerObservation:
			if v.Removed {
				s.log.Info("peer removed", "node_id", v.Peer.ID)
			} else {
				s.log.Info("peer added", "node_id", v.Peer.ID, "raft_addr", v.Peer.Address)
			}
		case raft.FailedHeartbeatObservation:
			s.log.Warn("peer unreachable", "node_id", v.PeerID, "last_contact", v.LastContact)
		case raft.ResumedHeartbeatObservation:
			s.log.Info("peer recovered", "node_id", v.PeerID)
		}
	}
}
