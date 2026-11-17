package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// 主备模式：主设备每次写入后把 (pos, cmd) 追加到内存日志；备设备长轮询 /_cluster/log 拉取，
// 它请求时带上的 after 就是确认位置。日志只保留最近 maxLogEntries 条，落后更多（或主设备重启过）
// 的备设备会收到 410，转而拉取完整快照。

const (
	maxLogEntries = 20000
	pollWait      = 20 * time.Second
	ackWait       = 2 * time.Second
)

type entry struct {
	Pos uint64 `json:"pos"`
	Cmd []byte `json:"cmd"`
}

type plog struct {
	mu       sync.Mutex
	cond     *sync.Cond
	base     uint64 // 日志第一条之前的位置
	entries  []entry
	ack      uint64
	lastPoll time.Time
	closed   bool
}

func newPlog(base uint64) *plog {
	l := &plog{base: base, ack: base}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *plog) last() uint64 {
	if len(l.entries) == 0 {
		return l.base
	}
	return l.entries[len(l.entries)-1].Pos
}

func (l *plog) append(p uint64, cmd []byte) {
	l.mu.Lock()
	l.entries = append(l.entries, entry{Pos: p, Cmd: cmd})
	if over := len(l.entries) - maxLogEntries; over > 0 {
		l.base = l.entries[over-1].Pos
		l.entries = append([]entry(nil), l.entries[over:]...)
	}
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *plog) close() {
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *plog) lag() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n uint64
	for i := len(l.entries) - 1; i >= 0 && l.entries[i].Pos > l.ack; i-- {
		n++
	}
	return n
}

// replicaAlive 表示备设备最近在拉日志。
func (l *plog) replicaAlive() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return time.Since(l.lastPoll) < 3*pollWait/2
}

// waitAck 等待备设备确认到 p，最多 d。
func (l *plog) waitAck(p uint64, d time.Duration) bool {
	deadline := time.Now().Add(d)
	t := time.AfterFunc(d, func() { l.mu.Lock(); l.cond.Broadcast(); l.mu.Unlock() })
	defer t.Stop()
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.ack < p && !l.closed && time.Now().Before(deadline) {
		l.cond.Wait()
	}
	return l.ack >= p
}

// since 返回 after 之后的条目；ok=false 表示 after 不在日志范围内，需要快照。
// 没有新条目时最多等待 wait。
func (l *plog) since(after uint64, wait time.Duration) (es []entry, ok bool) {
	deadline := time.Now().Add(wait)
	t := time.AfterFunc(wait, func() { l.mu.Lock(); l.cond.Broadcast(); l.mu.Unlock() })
	defer t.Stop()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastPoll = time.Now()
	if after > l.ack && after <= l.last() {
		l.ack = after
		l.cond.Broadcast()
	}
	for {
		if after == l.last() {
			if l.closed || !time.Now().Before(deadline) {
				return nil, true
			}
			l.cond.Wait()
			l.lastPoll = time.Now()
			continue
		}
		if after == l.base {
			return append([]entry(nil), l.entries...), true
		}
		for i, e := range l.entries {
			if e.Pos == after {
				return append([]entry(nil), l.entries[i+1:]...), true
			}
		}
		return nil, false
	}
}

type logResp struct {
	Epoch   uint64  `json:"epoch"`
	Entries []entry `json:"entries"`
}

// handleLog 由主设备提供。
func (n *Node) handleLog(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	epoch, _ := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
	n.mu.Lock()
	l := n.plog
	myEpoch := n.p.Epoch
	in := n.infoLocked()
	n.mu.Unlock()
	if l == nil || epoch != myEpoch {
		writeJSON(w, http.StatusConflict, in)
		return
	}
	es, ok := l.since(after, pollWait)
	if !ok {
		http.Error(w, "需要快照", http.StatusGone)
		return
	}
	writeJSON(w, http.StatusOK, logResp{Epoch: myEpoch, Entries: es})
}

// handleSnapshot 输出完整快照，任何角色都可提供。
func (n *Node) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := n.fsm.Snapshot(w); err != nil {
		n.cfg.logf("输出快照失败: %v", err)
	}
}

// followLoop 是备设备的复制循环。
func (n *Node) followLoop(ctx context.Context, addr string, epoch uint64) {
	client := httpClientFor(n.identity, pollWait+15*time.Second)
	backoff := time.Second
	for ctx.Err() == nil {
		err := n.pollOnce(ctx, client, addr, epoch)
		if err == nil {
			backoff = time.Second
			continue
		}
		if ctx.Err() != nil {
			return
		}
		n.cfg.logf("从主设备 %s 复制失败: %v", addr, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (n *Node) pollOnce(ctx context.Context, client *http.Client, addr string, epoch uint64) error {
	q := url.Values{"after": {strconv.FormatUint(n.followAfter(), 10)}, "epoch": {strconv.FormatUint(epoch, 10)}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+"/_cluster/log?"+q.Encode(), nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var lr logResp
		if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
			return err
		}
		for _, e := range lr.Entries {
			if e.Pos <= n.fsm.Applied() {
				continue
			}
			if _, err := n.fsm.Apply(e.Pos, e.Cmd); err != nil && !isAppErr(err) {
				return err
			}
		}
		return nil
	case http.StatusGone:
		n.cfg.logf("落后太多，从主设备拉取完整快照")
		return n.restoreFrom(ctx, client, addr)
	case http.StatusConflict:
		var in Info
		if err := json.NewDecoder(resp.Body).Decode(&in); err != nil {
			return err
		}
		go n.consider(in)
		return fmt.Errorf("主设备状态不一致 (对方 epoch %d, 角色 %s)", in.Epoch, in.Role)
	}
	return fmt.Errorf("HTTP %d", resp.StatusCode)
}

// restoreFrom 从指定节点拉取快照并恢复。
func (n *Node) restoreFrom(ctx context.Context, client *http.Client, addr string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+"/_cluster/snapshot", nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("拉取快照: HTTP %d", resp.StatusCode)
	}
	if err := n.fsm.Restore(resp.Body); err != nil {
		return fmt.Errorf("恢复快照: %w", err)
	}
	n.resyncDone()
	n.cfg.logf("已从 %s 恢复快照，位置 %d", addr, n.fsm.Applied())
	return nil
}
