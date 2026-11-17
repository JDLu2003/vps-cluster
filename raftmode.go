package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Raft 模式：每个 epoch 用一个新的 raft 目录；日志条目的 pos = epoch<<40 | raft index。
// 每当某个节点成为 leader，它先 Barrier 再写一条 baseline（带上自己的已应用位置）；
// 各节点应用 baseline 时若自己的位置不同，就从 leader 拉取完整快照。
// 这样新加入的节点、模式切换前落后的节点都能自动对齐。

func (n *Node) raftDir(epoch uint64) string {
	return filepath.Join(n.cfg.DataDir, "raft", strconv.FormatUint(epoch, 10))
}

// startRaftLocked 调用方持有 n.mu。
func (n *Node) startRaftLocked(bootstrap bool) error {
	epoch := n.p.Epoch
	dir := n.raftDir(epoch)
	// 清理旧 epoch 的目录
	if olds, _ := filepath.Glob(filepath.Join(n.cfg.DataDir, "raft", "*")); len(olds) > 0 {
		for _, o := range olds {
			if o != dir {
				os.RemoveAll(o)
			}
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return err
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 2, hclog.NewNullLogger())
	if err != nil {
		store.Close()
		return err
	}
	ln := n.mux.newRaftListener()
	stream := &raftStream{chanListener: ln, n: n, advertise: strAddr(n.cfg.Advertise)}
	trans := raft.NewNetworkTransportWithLogger(stream, 3, 10*time.Second, hclog.NewNullLogger())

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(n.p.NodeID)
	c.NoSnapshotRestoreOnStart = true // 状态机自己持久化，重启后靠 pos 跳过已应用的日志
	c.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft-" + n.cfg.Service, Level: hclog.Warn})
	c.SnapshotThreshold = 2048
	c.TrailingLogs = 4096
	// 跨公网的机器，适当放宽超时
	c.HeartbeatTimeout = 2 * time.Second
	c.ElectionTimeout = 2 * time.Second
	c.LeaderLeaseTimeout = time.Second
	c.CommitTimeout = 50 * time.Millisecond

	fsm := &raftFSM{n: n, epoch: epoch, ctx: n.modeCtx}
	r, err := raft.NewRaft(c, fsm, store, store, snaps, trans)
	if err != nil {
		trans.Close()
		store.Close()
		n.mux.closeRaftListener()
		return err
	}
	fsm.r = r
	if bootstrap {
		var servers []raft.Server
		for _, m := range n.p.Members {
			servers = append(servers, raft.Server{ID: raft.ServerID(m.ID), Address: raft.ServerAddress(m.Addr), Suffrage: raft.Voter})
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && err != raft.ErrCantBootstrap {
			n.cfg.logf("Raft bootstrap: %v", err)
		}
	}
	n.r, n.rstore, n.rtrans = r, store, trans
	n.cfg.logf("Raft 模式 (epoch %d, 成员 %d)", epoch, len(n.p.Members))

	n.modeWG.Add(1)
	go func(ctx context.Context) {
		defer n.modeWG.Done()
		n.leaderWatch(ctx, r)
	}(n.modeCtx)
	return nil
}

// leaderWatch 在本机成为 leader 时写入 baseline，并维护本地成员表。
func (n *Node) leaderWatch(ctx context.Context, r *raft.Raft) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case isLeader := <-r.LeaderCh():
			if isLeader {
				n.writeBaseline(ctx, r)
			}
		case <-t.C:
		}
		n.syncRaftMembers(r)
	}
}

func (n *Node) writeBaseline(ctx context.Context, r *raft.Raft) {
	// 模式切换时 teardown 持有 applyMu 并等待本 goroutine 退出，所以这里不能阻塞地加锁
	for !n.applyMu.TryLock() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	defer n.applyMu.Unlock()
	if err := r.Barrier(30 * time.Second).Error(); err != nil {
		n.cfg.logf("Raft barrier: %v", err)
		return
	}
	if err := r.Apply(baselineCmd(n.fsm.Applied()), 30*time.Second).Error(); err != nil {
		n.cfg.logf("写入 baseline: %v", err)
	}
}

// syncRaftMembers 把 raft 配置同步到本地 cluster.json，供重启和其他模式使用。
func (n *Node) syncRaftMembers(r *raft.Raft) {
	f := r.GetConfiguration()
	if f.Error() != nil {
		return
	}
	var ms []Member
	for _, s := range f.Configuration().Servers {
		ms = append(ms, Member{ID: string(s.ID), Addr: string(s.Address)})
	}
	if len(ms) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.r != r || sameMembers(ms, n.p.Members) {
		return
	}
	n.p.Members = ms
	if !n.isMember(n.p.NodeID) {
		n.p.Removed = true
	}
	_ = n.save()
}

func sameMembers(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[Member]bool{}
	for _, x := range a {
		m[x] = true
	}
	for _, x := range b {
		if !m[x] {
			return false
		}
	}
	return true
}

type raftFSM struct {
	n     *Node
	r     *raft.Raft
	epoch uint64
	ctx   context.Context
}

func (f *raftFSM) Apply(l *raft.Log) any {
	if l.Type != raft.LogCommand || len(l.Data) == 0 {
		return nil
	}
	p := pos(f.epoch, l.Index)
	applied := f.n.fsm.Applied()
	resync := f.n.resyncNeeded()
	if p <= applied && !resync {
		return applyResult{}
	}
	if resync && l.Data[0] == kindCmd {
		// 刚加入、尚未对齐：普通命令先跳过，等 baseline 触发快照
		return applyResult{}
	}
	switch l.Data[0] {
	case kindCmd:
		return resultOf(f.n.fsm.Apply(p, l.Data[1:]))
	case kindBaseline:
		if len(l.Data) != 9 {
			return nil
		}
		base := binary.BigEndian.Uint64(l.Data[1:])
		if applied != base || resync {
			f.n.cfg.logf("与 leader 状态不一致（本机 %d，leader %d），拉取快照", applied, base)
			f.resync()
		}
	}
	return nil
}

// resync 从 leader 拉快照，直到成功或模式结束。
func (f *raftFSM) resync() {
	client := httpClientFor(f.n.identity, 10*time.Minute)
	for f.ctx.Err() == nil {
		addr, id := f.r.LeaderWithID()
		if addr != "" && string(id) != f.n.self {
			if err := f.n.restoreFrom(f.ctx, client, string(addr)); err == nil {
				return
			} else {
				f.n.cfg.logf("拉取快照失败: %v", err)
			}
		}
		select {
		case <-f.ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func (f *raftFSM) Snapshot() (raft.FSMSnapshot, error) { return &raftSnap{fsm: f.n.fsm}, nil }

func (f *raftFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.n.fsm.Restore(rc); err != nil {
		return err
	}
	f.n.resyncDone()
	return nil
}

type raftSnap struct{ fsm FSM }

func (s *raftSnap) Persist(sink raft.SnapshotSink) error {
	if err := s.fsm.Snapshot(sink); err != nil {
		sink.Cancel()
		return fmt.Errorf("写快照: %w", err)
	}
	return sink.Close()
}

func (s *raftSnap) Release() {}
