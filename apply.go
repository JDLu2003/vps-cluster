package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// ErrNoWriter 表示当前找不到可写的节点（主设备失联、Raft 没有多数派、节点正在加入等）。
var ErrNoWriter = errors.New("集群暂时没有可写节点，改动无法保存")

// AppError 是状态机返回的业务错误（例如校验失败），各节点结果一致。
type AppError struct{ Msg string }

func (e *AppError) Error() string { return e.Msg }

func isAppErr(err error) bool {
	var ae *AppError
	return errors.As(err, &ae)
}

func appErr(err error) error {
	if err == nil {
		return nil
	}
	if isAppErr(err) {
		return err
	}
	return &AppError{Msg: err.Error()}
}

// raft 日志条目的第一个字节
const (
	kindCmd      byte = 0
	kindBaseline byte = 1
)

type applyResult struct {
	Data []byte `json:"data,omitempty"`
	Err  string `json:"err,omitempty"`
}

func (r applyResult) unwrap() ([]byte, error) {
	if r.Err != "" {
		return r.Data, &AppError{Msg: r.Err}
	}
	return r.Data, nil
}

func resultOf(data []byte, err error) applyResult {
	if err != nil {
		return applyResult{Data: data, Err: err.Error()}
	}
	return applyResult{Data: data}
}

// Apply 把命令提交到集群：本机是写节点则直接执行，否则转发给写节点。
// 返回值中状态机的业务错误为 *AppError；其他错误表示命令未必被执行。
func (n *Node) Apply(ctx context.Context, cmd []byte) ([]byte, error) {
	n.mu.Lock()
	in := n.infoLocked()
	r := n.r
	n.mu.Unlock()
	switch {
	case in.Joining || in.Removed:
		return nil, ErrNoWriter
	case in.Role == "single" || in.Role == "primary":
		return n.applyLocal(cmd)
	case in.Role == "leader" && r != nil:
		return n.applyRaft(r, cmd)
	}
	_, addr, ok := n.WriterAddr()
	if !ok {
		return nil, ErrNoWriter
	}
	return n.forwardApply(ctx, addr, cmd)
}

func (n *Node) applyLocal(cmd []byte) ([]byte, error) {
	n.applyMu.Lock()
	n.mu.Lock()
	mode := modeOf(n.p.Members)
	writer := !n.p.Removed && (mode == ModeSingle || (mode == ModePrimary && n.p.Primary == n.p.NodeID))
	l := n.plog
	epoch := n.p.Epoch
	n.mu.Unlock()
	if !writer {
		n.applyMu.Unlock()
		return nil, ErrNoWriter
	}
	a := n.fsm.Applied()
	p := pos(epoch, 1)
	if EpochOf(a) == epoch {
		p = a + 1
	}
	data, err := n.fsm.Apply(p, cmd)
	if l != nil {
		l.append(p, cmd)
	}
	n.applyMu.Unlock()
	if l != nil && l.replicaAlive() {
		l.waitAck(p, ackWait)
	}
	return data, appErr(err)
}

func (n *Node) applyRaft(r *raft.Raft, cmd []byte) ([]byte, error) {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	f := r.Apply(append([]byte{kindCmd}, cmd...), 15*time.Second)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, ErrNoWriter
		}
		return nil, err
	}
	res, _ := f.Response().(applyResult)
	return res.unwrap()
}

func (n *Node) forwardApply(ctx context.Context, addr string, cmd []byte) ([]byte, error) {
	var res applyResult
	if err := n.call(ctx, addr, "/_cluster/apply", cmd, &res); err != nil {
		if errors.Is(err, errNotWriter) {
			return nil, ErrNoWriter
		}
		return nil, fmt.Errorf("%w: %v", ErrNoWriter, err)
	}
	return res.unwrap()
}

var errNotWriter = errors.New("对方不是写节点")

// handleApply 只在写节点上执行，不再二次转发，避免环路。
func (n *Node) handleApply(w http.ResponseWriter, r *http.Request) {
	cmd, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	in := n.infoLocked()
	rf := n.r
	n.mu.Unlock()
	var data []byte
	switch {
	case in.Role == "single" || in.Role == "primary":
		data, err = n.applyLocal(cmd)
	case in.Role == "leader" && rf != nil:
		data, err = n.applyRaft(rf, cmd)
	default:
		http.Error(w, errNotWriter.Error(), http.StatusMisdirectedRequest)
		return
	}
	if err != nil && !isAppErr(err) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, resultOf(data, err))
}

// call 向某个节点发起 JSON（或原始字节）POST；body 为 nil 时发 GET。
func (n *Node) call(ctx context.Context, addr, path string, body any, out any) error {
	return doCall(ctx, n.client, addr, path, body, out)
}

func doCall(ctx context.Context, client *http.Client, addr, path string, body any, out any) error {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		buf, err := json.Marshal(b)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(buf)
	}
	method := http.MethodPost
	if body == nil {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://"+addr+path, rd)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusMisdirectedRequest {
		return errNotWriter
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s: HTTP %d %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (n *Node) fetchInfo(ctx context.Context, addr string) (*Info, error) {
	var in Info
	if err := n.call(ctx, addr, "/_cluster/info", nil, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func baselineCmd(base uint64) []byte {
	b := make([]byte, 9)
	b[0] = kindBaseline
	binary.BigEndian.PutUint64(b[1:], base)
	return b
}
