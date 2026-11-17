package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// ---------- 加入 ----------

func (n *Node) joinLoop(peers []string) {
	backoff := 2 * time.Second
	for n.ctx.Err() == nil {
		for _, addr := range peers {
			if addr == n.cfg.Advertise {
				continue
			}
			ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
			var in Info
			err := n.call(ctx, addr, "/_cluster/join", Member{ID: n.self, Addr: n.cfg.Advertise}, &in)
			cancel()
			if err == nil {
				n.finishJoin(in)
				return
			}
			n.mu.Lock()
			n.lastJoin = fmt.Sprintf("通过 %s 加入失败: %v", addr, err)
			n.mu.Unlock()
			n.cfg.logf("通过 %s 加入集群失败: %v", addr, err)
		}
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func (n *Node) finishJoin(in Info) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.teardownLocked()
	n.p.Epoch, n.p.Members, n.p.Primary, n.p.Removed = in.Epoch, in.Members, in.Primary, false
	n.p.Resync = true // 本地状态来自别处，必须以写节点的快照为准
	n.joining, n.lastJoin = false, ""
	_ = n.save()
	n.cfg.logf("已加入集群：模式 %s，成员 %d，epoch %d", modeOf(in.Members), len(in.Members), in.Epoch)
	n.setupLocked(false)
}

// resyncNeeded / resyncDone 管理"加入后必须先拉快照"的标记。
func (n *Node) resyncNeeded() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.p.Resync
}

func (n *Node) resyncDone() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.p.Resync {
		n.p.Resync = false
		_ = n.save()
	}
}

// followAfter 是备设备请求日志时使用的位置；需要强制快照时用一个不可能存在的位置。
func (n *Node) followAfter() uint64 {
	if n.resyncNeeded() {
		return math.MaxUint64
	}
	return n.fsm.Applied()
}

func (n *Node) handleJoin(w http.ResponseWriter, r *http.Request) {
	var m Member
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil || m.ID == "" || m.Addr == "" {
		http.Error(w, "参数错误", http.StatusBadRequest)
		return
	}
	if _, _, err := net.SplitHostPort(m.Addr); err != nil {
		http.Error(w, "地址无效", http.StatusBadRequest)
		return
	}
	in, err := n.viaWriter(r.Context(), "/_cluster/join", m, func() (Info, error) { return n.addMember(m) })
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, in)
}

// viaWriter 在写节点上执行 local，否则把请求转给写节点。
func (n *Node) viaWriter(ctx context.Context, path string, body any, local func() (Info, error)) (Info, error) {
	if n.IsWriter() {
		in, err := local()
		if !errors.Is(err, errNotWriter) {
			return in, err
		}
	}
	_, addr, ok := n.WriterAddr()
	if !ok {
		return Info{}, ErrNoWriter
	}
	var in Info
	err := n.call(ctx, addr, path, body, &in)
	return in, err
}

type notice struct {
	addr string
	path string
	in   Info
}

func (n *Node) send(ns []notice) {
	for _, x := range ns {
		if x.addr == "" || x.addr == n.cfg.Advertise {
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		if err := n.call(ctx, x.addr, x.path, x.in, nil); err != nil {
			n.cfg.logf("通知 %s 失败（对方稍后会通过状态检查自动同步）: %v", x.addr, err)
		}
		cancel()
	}
}

// addMember 在写节点上执行。
func (n *Node) addMember(m Member) (Info, error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	var ns []notice
	defer func() { go n.send(ns) }()
	defer n.mu.Unlock()
	if n.p.Removed {
		return Info{}, errNotWriter
	}
	mode := modeOf(n.p.Members)
	if mode != ModeRaft && n.p.Primary != n.self {
		return Info{}, errNotWriter
	}
	if old := n.memberAddr(m.ID); old == m.Addr {
		return n.infoLocked(), nil
	} else if old != "" && mode != ModeRaft {
		// 已是成员，只是地址变了
		for i := range n.p.Members {
			if n.p.Members[i].ID == m.ID {
				n.p.Members[i].Addr = m.Addr
			}
		}
		_ = n.save()
		n.teardownLocked()
		n.setupLocked(false)
		return n.infoLocked(), nil
	}
	switch mode {
	case ModeSingle:
		n.teardownLocked()
		n.p.Epoch++
		n.p.Members = append(n.p.Members, m)
		n.p.Primary = n.self
		_ = n.save()
		n.setupLocked(false)
		n.cfg.logf("机器 %s 加入：切换为主备模式，本机为主设备", m.ID)
	case ModePrimary:
		var other Member
		for _, x := range n.p.Members {
			if x.ID != n.self {
				other = x
			}
		}
		if l := n.plog; l != nil {
			last := l.last()
			n.mu.Unlock()
			l.waitAck(last, 5*time.Second)
			n.mu.Lock()
		}
		n.teardownLocked()
		n.p.Epoch++
		n.p.Members = append(n.p.Members, m)
		_ = n.save()
		n.setupLocked(true)
		ns = append(ns, notice{addr: other.Addr, path: "/_cluster/adopt", in: n.infoLocked()})
		n.cfg.logf("机器 %s 加入：切换为 Raft 模式（%d 台）", m.ID, len(n.p.Members))
	case ModeRaft:
		r := n.r
		if r == nil || r.State() != raft.Leader {
			return Info{}, errNotWriter
		}
		n.mu.Unlock()
		err := r.AddVoter(raft.ServerID(m.ID), raft.ServerAddress(m.Addr), 0, 30*time.Second).Error()
		if err == nil {
			// 让新成员尽快对齐（它会因 baseline 拉取快照）
			_ = r.Apply(baselineCmd(n.fsm.Applied()), 30*time.Second).Error()
		}
		n.mu.Lock()
		if err != nil {
			return Info{}, fmt.Errorf("Raft 添加成员: %w", err)
		}
		n.p.Members = append(withoutMember(n.p.Members, m.ID), m)
		_ = n.save()
		n.cfg.logf("机器 %s 加入 Raft 集群", m.ID)
	}
	return n.infoLocked(), nil
}

func withoutMember(ms []Member, id string) []Member {
	var out []Member
	for _, m := range ms {
		if m.ID != id {
			out = append(out, m)
		}
	}
	return out
}

// ---------- 移除 ----------

// RemoveMember 把一台机器移出集群（必要时自动切换模式）。不能移除当前写节点。
func (n *Node) RemoveMember(ctx context.Context, id string) error {
	_, err := n.viaWriter(ctx, "/_cluster/remove", map[string]string{"id": id}, func() (Info, error) { return n.removeMember(id) })
	return err
}

func (n *Node) handleRemove(w http.ResponseWriter, r *http.Request) {
	var req struct{ ID string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "参数错误", http.StatusBadRequest)
		return
	}
	in, err := n.viaWriter(r.Context(), "/_cluster/remove", map[string]string{"id": req.ID}, func() (Info, error) { return n.removeMember(req.ID) })
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (n *Node) removeMember(id string) (Info, error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	var ns []notice
	defer func() { go n.send(ns) }()
	defer n.mu.Unlock()
	if id == n.self {
		return Info{}, errors.New("不能移除当前写节点，请先把写节点切换到其他机器")
	}
	if !n.isMember(id) {
		return n.infoLocked(), nil
	}
	removedAddr := n.memberAddr(id)
	switch modeOf(n.p.Members) {
	case ModeSingle:
		return n.infoLocked(), nil
	case ModePrimary:
		if n.p.Primary != n.self {
			return Info{}, errNotWriter
		}
		n.teardownLocked()
		n.p.Epoch++
		n.p.Members = withoutMember(n.p.Members, id)
		_ = n.save()
		n.setupLocked(false)
		n.cfg.logf("机器 %s 已移除：切换为单机模式", id)
	case ModeRaft:
		r := n.r
		if r == nil || r.State() != raft.Leader {
			return Info{}, errNotWriter
		}
		n.mu.Unlock()
		err := r.RemoveServer(raft.ServerID(id), 0, 30*time.Second).Error()
		n.mu.Lock()
		if err != nil {
			return Info{}, fmt.Errorf("Raft 移除成员: %w", err)
		}
		remaining := withoutMember(n.p.Members, id)
		if len(remaining) >= 3 {
			n.p.Members = remaining
			_ = n.save()
			n.cfg.logf("机器 %s 已移出 Raft 集群", id)
			break
		}
		n.mu.Unlock()
		_ = r.Barrier(10 * time.Second).Error()
		n.mu.Lock()
		n.teardownLocked()
		n.p.Epoch++
		n.p.Members = remaining
		n.p.Primary = n.self
		_ = n.save()
		n.setupLocked(false)
		in := n.infoLocked()
		for _, m := range remaining {
			ns = append(ns, notice{addr: m.Addr, path: "/_cluster/adopt", in: in})
		}
		n.cfg.logf("机器 %s 已移除：切换为主备模式，本机为主设备", id)
	}
	ns = append(ns, notice{addr: removedAddr, path: "/_cluster/removed", in: n.infoLocked()})
	return n.infoLocked(), nil
}

func (n *Node) handleRemoved(w http.ResponseWriter, r *http.Request) {
	var in Info
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "参数错误", http.StatusBadRequest)
		return
	}
	n.applyMu.Lock()
	n.mu.Lock()
	if in.Epoch >= n.p.Epoch && !containsMember(in.Members, n.self) {
		n.teardownLocked()
		n.p.Removed = true
		n.p.Epoch = in.Epoch
		_ = n.save()
		n.setupLocked(false)
	}
	n.mu.Unlock()
	n.applyMu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func containsMember(ms []Member, id string) bool {
	for _, m := range ms {
		if m.ID == id {
			return true
		}
	}
	return false
}

// ---------- 切换写节点 ----------

// SetWriter 把写节点切换到指定机器：主备模式下交换主备角色，Raft 模式下转移 leader。
func (n *Node) SetWriter(ctx context.Context, id string) error {
	_, err := n.viaWriter(ctx, "/_cluster/writer", map[string]string{"id": id}, func() (Info, error) { return n.setWriter(id) })
	return err
}

func (n *Node) handleWriter(w http.ResponseWriter, r *http.Request) {
	var req struct{ ID string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "参数错误", http.StatusBadRequest)
		return
	}
	in, err := n.viaWriter(r.Context(), "/_cluster/writer", map[string]string{"id": req.ID}, func() (Info, error) { return n.setWriter(req.ID) })
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, in)
}

func (n *Node) setWriter(id string) (Info, error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	var ns []notice
	defer func() { go n.send(ns) }()
	defer n.mu.Unlock()
	if !n.isMember(id) {
		return Info{}, errors.New("目标不是集群成员")
	}
	switch modeOf(n.p.Members) {
	case ModeSingle:
		return n.infoLocked(), nil
	case ModePrimary:
		if n.p.Primary != n.self {
			return Info{}, errNotWriter
		}
		if id == n.self {
			return n.infoLocked(), nil
		}
		if l := n.plog; l != nil {
			last := l.last()
			n.mu.Unlock()
			ok := l.waitAck(last, 10*time.Second)
			n.mu.Lock()
			if !ok {
				return Info{}, errors.New("备设备尚未追平（可能失联），暂不能切换")
			}
		}
		n.teardownLocked()
		n.p.Epoch++
		n.p.Primary = id
		_ = n.save()
		n.setupLocked(false)
		ns = append(ns, notice{addr: n.memberAddr(id), path: "/_cluster/adopt", in: n.infoLocked()})
		n.cfg.logf("主设备切换到 %s", id)
	case ModeRaft:
		r := n.r
		if r == nil || r.State() != raft.Leader {
			return Info{}, errNotWriter
		}
		if id == n.self {
			return n.infoLocked(), nil
		}
		addr := n.memberAddr(id)
		n.mu.Unlock()
		err := r.LeadershipTransferToServer(raft.ServerID(id), raft.ServerAddress(addr)).Error()
		n.mu.Lock()
		if err != nil {
			return Info{}, fmt.Errorf("转移 leader: %w", err)
		}
	}
	return n.infoLocked(), nil
}

// ForcePromote 在写节点失联时，让本机强行成为写节点：
// 主备模式下本机成为主设备；Raft 模式（失去多数派）或已被移除时，本机独立成为单机集群。
// 旧写节点恢复后会看到更高的 epoch 并自动降级，其未复制的写入会被丢弃。
func (n *Node) ForcePromote() error {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	var ns []notice
	defer func() { go n.send(ns) }()
	defer n.mu.Unlock()
	if n.joining {
		return errors.New("正在加入集群，不能强制提升")
	}
	old := append([]Member(nil), n.p.Members...)
	n.teardownLocked()
	n.p.Epoch++
	n.p.Primary = n.self
	if modeOf(n.p.Members) != ModePrimary || n.p.Removed {
		n.p.Members = []Member{{ID: n.self, Addr: n.cfg.Advertise}}
	}
	n.p.Removed = false
	n.p.Resync = false
	_ = n.save()
	n.setupLocked(false)
	in := n.infoLocked()
	for _, m := range old {
		ns = append(ns, notice{addr: m.Addr, path: "/_cluster/adopt", in: in})
	}
	n.cfg.logf("已强制提升为写节点 (epoch %d, 模式 %s)", n.p.Epoch, modeOf(n.p.Members))
	return nil
}

// ---------- 状态传播 ----------

func (n *Node) handleAdopt(w http.ResponseWriter, r *http.Request) {
	var in Info
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "参数错误", http.StatusBadRequest)
		return
	}
	n.consider(in)
	w.WriteHeader(http.StatusOK)
}

// consider 看到别的节点的状态：epoch 更高就照它的成员表和写节点重新配置自己。
// 同 epoch 下两个节点都自认主设备（两边同时强制提升）时，ID 小的一方胜出。
func (n *Node) consider(in Info) {
	if in.Service != "" && in.Service != n.cfg.Service {
		return
	}
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.joining || len(in.Members) == 0 {
		return
	}
	newer := in.Epoch > n.p.Epoch
	tie := in.Epoch == n.p.Epoch && in.Mode == ModePrimary && modeOf(n.p.Members) == ModePrimary &&
		in.Primary != n.p.Primary && in.Primary == in.NodeID && in.Primary < n.p.Primary
	if !newer && !tie {
		return
	}
	if in.Removed && !newer {
		return
	}
	// 对方自己已被移除时，它的成员表不可信
	if in.Removed {
		return
	}
	n.teardownLocked()
	n.p.Epoch, n.p.Members, n.p.Primary = in.Epoch, in.Members, in.Primary
	n.p.Removed = !containsMember(in.Members, n.self)
	_ = n.save()
	n.cfg.logf("采用集群新状态：epoch %d，模式 %s，写节点 %s", in.Epoch, modeOf(in.Members), in.Writer)
	n.setupLocked(false)
}

// GossipInterval 是节点间互相检查集群状态的间隔（测试中可调小）。
var GossipInterval = 15 * time.Second

// gossipLoop 定期询问其他成员，发现更新的集群状态。
func (n *Node) gossipLoop() {
	t := time.NewTicker(GossipInterval)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		in := n.Info()
		if in.Joining {
			continue
		}
		for _, m := range in.Members {
			if m.ID == in.NodeID {
				continue
			}
			ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
			other, err := n.fetchInfo(ctx, m.Addr)
			cancel()
			if err == nil {
				n.consider(*other)
			}
		}
	}
}

// ---------- HTTP ----------

func (n *Node) routes() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /_cluster/info", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, n.Info()) })
	m.HandleFunc("GET /_cluster/log", n.handleLog)
	m.HandleFunc("GET /_cluster/snapshot", n.handleSnapshot)
	m.HandleFunc("POST /_cluster/apply", n.handleApply)
	m.HandleFunc("POST /_cluster/join", n.handleJoin)
	m.HandleFunc("POST /_cluster/remove", n.handleRemove)
	m.HandleFunc("POST /_cluster/writer", n.handleWriter)
	m.HandleFunc("POST /_cluster/adopt", n.handleAdopt)
	m.HandleFunc("POST /_cluster/removed", n.handleRemoved)
	m.Handle("/_app/", http.StripPrefix("/_app", n.appMux))
	m.HandleFunc("/_fwd/", func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		h := n.fwd
		n.mu.Unlock()
		if h == nil {
			http.NotFound(w, r)
			return
		}
		r2 := r.Clone(context.WithValue(r.Context(), fwdKey{}, true))
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/_fwd")
		r2.URL.RawPath = ""
		r2.RequestURI = r2.URL.RequestURI()
		if ip := r.Header.Get(fwdIPHeader); ip != "" {
			r2.RemoteAddr = net.JoinHostPort(ip, "0")
		}
		h.ServeHTTP(w, r2)
	})
	return m
}

// Handle 在集群端口上注册服务自己的内部接口，路径前缀为 /_app。
// 只有持有集群身份的节点能访问。
func (n *Node) Handle(pattern string, h http.Handler) { n.appMux.Handle(pattern, h) }

// HTTPClient 返回带集群身份的 HTTP 客户端，用来访问 https://<成员地址>/_app/...。
func (n *Node) HTTPClient() *http.Client { return n.client }

type fwdKey struct{}

const fwdIPHeader = "X-Cluster-Client-IP"

// SetForwardHandler 设置写节点上处理被转发请求的 handler（通常就是服务的公开 handler）。
func (n *Node) SetForwardHandler(h http.Handler) {
	n.mu.Lock()
	n.fwd = h
	n.mu.Unlock()
}

// Forwarded 判断请求是否是其他节点转发来的。
func Forwarded(r *http.Request) bool { v, _ := r.Context().Value(fwdKey{}).(bool); return v }

// ProxyToWriter 把一个公开请求原样转给写节点处理（经集群端口）。clientIP 为原始客户端 IP。
func (n *Node) ProxyToWriter(w http.ResponseWriter, r *http.Request, clientIP string) {
	_, addr, ok := n.WriterAddr()
	if !ok {
		http.Error(w, ErrNoWriter.Error(), http.StatusServiceUnavailable)
		return
	}
	rp := &httputil.ReverseProxy{
		Transport: n.client.Transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "https"
			pr.Out.URL.Host = addr
			pr.Out.URL.Path = "/_fwd" + pr.In.URL.Path
			pr.Out.URL.RawPath = ""
			pr.Out.Host = addr
			pr.Out.Header.Set(fwdIPHeader, clientIP)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, ErrNoWriter.Error()+": "+err.Error(), http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r)
}
