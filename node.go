// Package cluster 让一个服务在 1 台、2 台、3 台及以上机器上复制自己的状态：
//
//   - 1 台：single，不做任何复制；
//   - 2 台：primary，主设备执行写入并把命令日志推给备设备；主设备可随时切换，主设备失联时可在备设备上强制提升；
//   - 3 台及以上：raft（hashicorp/raft），自动选主、自动故障转移。
//
// 模式完全由成员数决定，增删成员时由写节点自动切换。服务只需实现 FSM：
// 一个确定性的状态机，它自己持久化状态并记住已应用到的位置（pos）。
// pos = epoch<<40 | 序号，epoch 在每次模式切换时加一，所以 pos 在整个集群生命周期内单调递增，
// 状态机据此跳过重复的命令，复制层因此可以放心地"至少一次"投递。
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// FSM 是服务提供的状态机。
type FSM interface {
	// Apply 执行一条命令。必须是确定性的；无论成功与否都要把已应用位置推进到 pos 并持久化，
	// 返回的 error 视为命令的"结果"（各节点上一致），不是复制失败。
	Apply(pos uint64, cmd []byte) ([]byte, error)
	// Applied 返回已持久化的最后应用位置，从未应用过返回 0。
	Applied() uint64
	// Snapshot 写出完整状态（含已应用位置）。可能与 Apply 并发调用，实现方需要自行加锁。
	Snapshot(w io.Writer) error
	// Restore 用快照替换全部状态。
	Restore(r io.Reader) error
}

type Mode string

const (
	ModeSingle  Mode = "single"
	ModePrimary Mode = "primary"
	ModeRaft    Mode = "raft"
)

type Member struct {
	ID   string `json:"id"`
	Addr string `json:"addr"` // 集群端口 host:port
}

// persisted 保存在 DataDir/cluster.json。
type persisted struct {
	NodeID  string   `json:"node_id"`
	Seed    string   `json:"seed"`
	Epoch   uint64   `json:"epoch"`
	Members []Member `json:"members"`
	Primary string   `json:"primary"` // single/primary 模式下的写节点
	Removed bool     `json:"removed"`
	Resync  bool     `json:"resync,omitempty"` // 刚加入集群，必须先从写节点拉快照
}

func modeOf(members []Member) Mode {
	switch {
	case len(members) <= 1:
		return ModeSingle
	case len(members) == 2:
		return ModePrimary
	}
	return ModeRaft
}

type Config struct {
	Service   string // 服务名，用于派生证书，如 "vpnmgr"
	NodeID    string // 机器 ID；为空时首次启动随机生成
	Advertise string // 其他节点访问本节点集群端口的地址 host:port
	Listen    string // 集群端口监听地址，如 ":7946"
	DataDir   string
	FSM       FSM

	// Seed 非空时使用该集群身份；与本地记录不同且 Peers 非空时，自动通过 Peers 加入该集群。
	Seed  string
	Peers []string

	Logf func(format string, args ...any)
}

// Node 是本机在集群中的代表。
type Node struct {
	cfg Config
	fsm FSM

	applyMu sync.Mutex // 串行化本地写入与模式切换
	mu      sync.Mutex
	self    string // 本机 ID，加载后不再变化
	p       persisted
	id      atomic.Pointer[Identity]

	mux    *mux
	srv    *http.Server
	client *http.Client
	appMux *http.ServeMux
	fwd    http.Handler

	ctx    context.Context
	cancel context.CancelFunc

	// 当前模式的运行时
	modeCtx    context.Context
	modeCancel context.CancelFunc
	modeWG     sync.WaitGroup
	plog       *plog // primary 模式写节点
	r          *raft.Raft
	rstore     *raftboltdb.BoltStore
	rtrans     *raft.NetworkTransport

	joining  bool
	lastJoin string
}

func (c *Config) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
		return
	}
	log.Printf("[cluster/"+c.Service+"] "+format, args...)
}

func pos(epoch, idx uint64) uint64 { return epoch<<40 | idx }

// EpochOf 返回 pos 所属的 epoch。
func EpochOf(p uint64) uint64 { return p >> 40 }

// Start 启动集群端口并按本地记录的模式运行。
func Start(cfg Config) (*Node, error) {
	if cfg.Service == "" || cfg.DataDir == "" || cfg.FSM == nil || cfg.Advertise == "" {
		return nil, errors.New("cluster: Service、DataDir、FSM、Advertise 必填")
	}
	if cfg.Listen == "" {
		_, port, err := net.SplitHostPort(cfg.Advertise)
		if err != nil {
			return nil, fmt.Errorf("cluster: Advertise 地址无效: %w", err)
		}
		cfg.Listen = ":" + port
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, fsm: cfg.FSM, appMux: http.NewServeMux()}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.client = httpClientFor(n.identity, 30*time.Second)

	fresh, err := n.load()
	if err != nil {
		return nil, err
	}
	needJoin := len(cfg.Peers) > 0 && cfg.Seed != "" && (fresh || cfg.Seed != n.p.Seed || n.p.Removed)
	if cfg.Seed != "" && cfg.Seed != n.p.Seed {
		n.p.Seed = cfg.Seed
		if !needJoin {
			// 换成新的集群身份，但保持单机（例如把已有的 artifact 实例纳入统一身份）
			n.p.Epoch++
			if e := EpochOf(n.fsm.Applied()); n.p.Epoch <= e {
				n.p.Epoch = e + 1
			}
			n.p.Members = []Member{{ID: n.p.NodeID, Addr: cfg.Advertise}}
			n.p.Primary = n.p.NodeID
			n.p.Removed = false
		}
	}
	id, err := DeriveIdentity(cfg.Service, n.p.Seed)
	if err != nil {
		return nil, err
	}
	n.id.Store(id)
	n.self = n.p.NodeID
	// 状态机里可能已有更高 epoch 的数据（例如从备份恢复、或旧集群的数据），
	// 新的 epoch 必须更大，否则新写入的 pos 会小于已应用位置而被跳过。
	if e := EpochOf(n.fsm.Applied()); fresh && n.p.Epoch <= e {
		n.p.Epoch = e + 1
	}
	n.updateSelfAddr()
	if err := n.save(); err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("cluster: 监听 %s 失败: %w", cfg.Listen, err)
	}
	n.mux = &mux{n: n, ln: ln, http: newChanListener(ln.Addr())}
	n.srv = &http.Server{Handler: n.routes(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	go n.mux.serve()
	go n.srv.Serve(n.mux.http)

	if needJoin {
		n.joining = true
		go n.joinLoop(cfg.Peers)
	} else {
		n.mu.Lock()
		n.setupLocked(false)
		n.mu.Unlock()
	}
	go n.gossipLoop()
	return n, nil
}

func (n *Node) statePath() string { return filepath.Join(n.cfg.DataDir, "cluster.json") }

func (n *Node) load() (fresh bool, err error) {
	b, err := os.ReadFile(n.statePath())
	if errors.Is(err, os.ErrNotExist) {
		id := n.cfg.NodeID
		if id == "" {
			id = randHex(6)
		}
		seed := n.cfg.Seed
		if seed == "" {
			seed = NewSeed()
		}
		n.p = persisted{NodeID: id, Seed: seed, Epoch: 1, Members: []Member{{ID: id, Addr: n.cfg.Advertise}}, Primary: id}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &n.p); err != nil {
		return false, fmt.Errorf("解析 %s 失败: %w", n.statePath(), err)
	}
	if n.cfg.NodeID != "" && n.cfg.NodeID != n.p.NodeID {
		return false, fmt.Errorf("NODE_ID=%s 与本地集群记录 %s 不一致", n.cfg.NodeID, n.p.NodeID)
	}
	return false, nil
}

func (n *Node) save() error {
	b, _ := json.MarshalIndent(n.p, "", "  ")
	tmp := n.statePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, n.statePath())
}

// updateSelfAddr 在本机公网地址变化时更新成员表里自己的地址。
func (n *Node) updateSelfAddr() {
	for i := range n.p.Members {
		if n.p.Members[i].ID == n.p.NodeID {
			n.p.Members[i].Addr = n.cfg.Advertise
		}
	}
}

func (n *Node) identity() *Identity { return n.id.Load() }

// Close 停止复制和集群端口。
func (n *Node) Close() error {
	n.cancel()
	n.applyMu.Lock()
	n.mu.Lock()
	n.teardownLocked()
	n.mu.Unlock()
	n.applyMu.Unlock()
	n.srv.Close()
	n.mux.ln.Close()
	return nil
}

// ---------- 模式运行时 ----------

// setupLocked 按 n.p 启动对应模式。调用方持有 n.mu。
func (n *Node) setupLocked(bootstrap bool) {
	n.modeCtx, n.modeCancel = context.WithCancel(n.ctx)
	if n.p.Removed {
		n.cfg.logf("本节点已被移出集群，停止复制")
		return
	}
	switch modeOf(n.p.Members) {
	case ModeSingle:
		n.cfg.logf("单机模式 (epoch %d)", n.p.Epoch)
	case ModePrimary:
		if n.p.Primary == n.p.NodeID {
			n.plog = newPlog(n.fsm.Applied())
			n.cfg.logf("主备模式：本机为主设备 (epoch %d)", n.p.Epoch)
		} else {
			addr := n.memberAddr(n.p.Primary)
			epoch := n.p.Epoch
			n.cfg.logf("主备模式：本机为备设备，主设备 %s (epoch %d)", addr, epoch)
			n.modeWG.Add(1)
			go func(ctx context.Context) {
				defer n.modeWG.Done()
				n.followLoop(ctx, addr, epoch)
			}(n.modeCtx)
		}
	case ModeRaft:
		if err := n.startRaftLocked(bootstrap); err != nil {
			n.cfg.logf("启动 Raft 失败: %v", err)
		}
	}
}

// teardownLocked 停掉当前模式。调用方持有 n.mu（若会影响写入，还应持有 applyMu）。
func (n *Node) teardownLocked() {
	if n.modeCancel != nil {
		n.modeCancel()
	}
	if n.plog != nil {
		n.plog.close()
		n.plog = nil
	}
	if n.r != nil {
		r := n.r
		n.r = nil
		// 关闭 Raft 时其内部 goroutine 可能需要 n.mu（例如读身份），先解锁等待
		n.mu.Unlock()
		if err := r.Shutdown().Error(); err != nil {
			n.cfg.logf("关闭 Raft: %v", err)
		}
		n.mu.Lock()
		if n.rtrans != nil {
			n.rtrans.Close()
			n.rtrans = nil
		}
		if n.rstore != nil {
			n.rstore.Close()
			n.rstore = nil
		}
		n.mux.closeRaftListener()
	}
	n.mu.Unlock()
	n.modeWG.Wait()
	n.mu.Lock()
}

func (n *Node) memberAddr(id string) string {
	for _, m := range n.p.Members {
		if m.ID == id {
			return m.Addr
		}
	}
	return ""
}

func (n *Node) isMember(id string) bool { return n.memberAddr(id) != "" }

// ---------- 对外查询 ----------

// Info 是节点对外公布的状态。
type Info struct {
	Service string   `json:"service"`
	NodeID  string   `json:"node_id"`
	Addr    string   `json:"addr"`
	Epoch   uint64   `json:"epoch"`
	Mode    Mode     `json:"mode"`
	Members []Member `json:"members"`
	Primary string   `json:"primary,omitempty"`
	Removed bool     `json:"removed,omitempty"`
	Joining bool     `json:"joining,omitempty"`
	Role    string   `json:"role"`   // single | primary | replica | leader | follower | candidate | removed | joining
	Writer  string   `json:"writer"` // 当前写节点 ID，未知为空
	Applied uint64   `json:"applied"`
	Lag     uint64   `json:"lag,omitempty"` // 主设备视角：备设备落后的条数
	Err     string   `json:"err,omitempty"`
}

func (n *Node) Info() Info {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.infoLocked()
}

func (n *Node) infoLocked() Info {
	in := Info{Service: n.cfg.Service, NodeID: n.p.NodeID, Addr: n.cfg.Advertise, Epoch: n.p.Epoch, Mode: modeOf(n.p.Members),
		Members: append([]Member(nil), n.p.Members...), Primary: n.p.Primary, Removed: n.p.Removed, Joining: n.joining, Applied: n.fsm.Applied(), Err: n.lastJoin}
	switch {
	case n.joining:
		in.Role = "joining"
	case n.p.Removed:
		in.Role = "removed"
	case in.Mode == ModeSingle:
		in.Role, in.Writer = "single", n.p.NodeID
	case in.Mode == ModePrimary:
		in.Writer = n.p.Primary
		if n.p.Primary == n.p.NodeID {
			in.Role = "primary"
			if n.plog != nil {
				in.Lag = n.plog.lag()
			}
		} else {
			in.Role = "replica"
		}
	case n.r != nil:
		switch n.r.State() {
		case raft.Leader:
			in.Role = "leader"
		case raft.Candidate:
			in.Role = "candidate"
		default:
			in.Role = "follower"
		}
		_, lid := n.r.LeaderWithID()
		in.Writer = string(lid)
		if f := n.r.GetConfiguration(); f.Error() == nil {
			in.Members = nil
			for _, s := range f.Configuration().Servers {
				in.Members = append(in.Members, Member{ID: string(s.ID), Addr: string(s.Address)})
			}
		}
	default:
		in.Role = "follower"
	}
	sort.Slice(in.Members, func(i, j int) bool { return in.Members[i].ID < in.Members[j].ID })
	return in
}

func (n *Node) NodeID() string { return n.Info().NodeID }

// Seed 返回集群种子（用于生成加入令牌，只能给管理员看）。
func (n *Node) Seed() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.p.Seed
}

// JoinToken 生成加入本集群的令牌，peers 为所有成员的集群地址。
func (n *Node) JoinToken() string {
	in := n.Info()
	t := JoinToken{Seed: n.Seed()}
	for _, m := range in.Members {
		t.Peers = append(t.Peers, m.Addr)
	}
	return t.Encode()
}

// IsWriter 判断本机当前是否可以直接写入。
func (n *Node) IsWriter() bool {
	in := n.Info()
	return in.Writer == in.NodeID && in.Role != "removed" && in.Role != "joining"
}

// WriterAddr 返回写节点的集群地址。
func (n *Node) WriterAddr() (id, addr string, ok bool) {
	in := n.Info()
	if in.Writer == "" || in.Removed || in.Joining {
		return "", "", false
	}
	for _, m := range in.Members {
		if m.ID == in.Writer {
			return m.ID, m.Addr, true
		}
	}
	return "", "", false
}

// MemberStatus 是从某个成员那里实时取到的状态。
type MemberStatus struct {
	Member
	Info *Info  `json:"info,omitempty"`
	Err  string `json:"err,omitempty"`
}

// Status 并行询问所有成员，返回各自的实时状态。
func (n *Node) Status(ctx context.Context) []MemberStatus {
	self := n.Info()
	out := make([]MemberStatus, len(self.Members))
	var wg sync.WaitGroup
	for i, m := range self.Members {
		out[i].Member = m
		if m.ID == self.NodeID {
			s := self
			out[i].Info = &s
			continue
		}
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			in, err := n.fetchInfo(cctx, addr)
			if err != nil {
				out[i].Err = err.Error()
				return
			}
			out[i].Info = in
		}(i, m.Addr)
	}
	wg.Wait()
	return out
}

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
