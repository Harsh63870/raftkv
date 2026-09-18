// Package fsm implements the replicated state machine: a string->string map
// rebuilt by replaying the raft log.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "del"
	OpPeer   Op = "peer"
)

type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
}

func (c Command) Encode() ([]byte, error) { return json.Marshal(c) }

type state struct {
	KV    map[string]string `json:"kv"`
	Peers map[string]string `json:"peers"`
}

type FSM struct {
	log *slog.Logger

	mu sync.RWMutex
	kv map[string]string
	// node id -> gRPC address. Replicated through the log so every node can
	// resolve the leader's client address without an out-of-band registry.
	peers map[string]string
}

func New(log *slog.Logger) *FSM {
	return &FSM{
		log:   log,
		kv:    map[string]string{},
		peers: map[string]string{},
	}
}

func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.kv[key]
	return v, ok
}

func (f *FSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.kv)
}

func (f *FSM) PeerAddr(id string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.peers[id]
}

func (f *FSM) Peers() map[string]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return maps.Clone(f.peers)
}

func (f *FSM) Apply(l *raft.Log) any {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		f.log.Error("apply: malformed entry", "index", l.Index, "term", l.Term, "err", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch c.Op {
	case OpSet:
		f.kv[c.Key] = c.Value
	case OpDelete:
		delete(f.kv, c.Key)
	case OpPeer:
		f.peers[c.Key] = c.Value
	default:
		err := fmt.Errorf("unknown op %q", c.Op)
		f.log.Error("apply: rejected entry", "index", l.Index, "err", err)
		return err
	}
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	b, err := json.Marshal(state{KV: f.kv, Peers: f.peers})
	if err != nil {
		return nil, err
	}
	return &snapshot{data: b}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var st state
	if err := json.NewDecoder(rc).Decode(&st); err != nil {
		return err
	}
	if st.KV == nil {
		st.KV = map[string]string{}
	}
	if st.Peers == nil {
		st.Peers = map[string]string{}
	}

	f.mu.Lock()
	f.kv, f.peers = st.KV, st.Peers
	f.mu.Unlock()

	f.log.Info("restored from snapshot", "keys", len(st.KV), "peers", len(st.Peers))
	return nil
}

type snapshot struct{ data []byte }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
