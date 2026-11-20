package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// kvFSM 是测试用的状态机：命令 "k=v"，状态持久化到 JSON 文件。
type kvFSM struct {
	mu   sync.Mutex
	path string
	st   kvState
}

type kvState struct {
	Applied uint64            `json:"applied"`
	KV      map[string]string `json:"kv"`
}

func newKV(dir string) *kvFSM {
	f := &kvFSM{path: filepath.Join(dir, "kv.json"), st: kvState{KV: map[string]string{}}}
	if b, err := os.ReadFile(f.path); err == nil {
		_ = json.Unmarshal(b, &f.st)
	}
	return f
}

func (f *kvFSM) persist() {
	b, _ := json.Marshal(f.st)
	_ = os.WriteFile(f.path, b, 0o600)
}

func (f *kvFSM) Apply(p uint64, cmd []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st.Applied = p
	defer f.persist()
	var k, v string
	if _, err := fmt.Sscanf(string(cmd), "%s %s", &k, &v); err != nil {
		return nil, fmt.Errorf("bad cmd")
	}
	f.st.KV[k] = v
	return []byte("ok"), nil
}

func (f *kvFSM) Applied() uint64 { f.mu.Lock(); defer f.mu.Unlock(); return f.st.Applied }

func (f *kvFSM) Snapshot(w io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.NewEncoder(w).Encode(f.st)
}

func (f *kvFSM) Restore(r io.Reader) error {
	var st kvState
	if err := json.NewDecoder(r).Decode(&st); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st = st
	f.persist()
	return nil
}

func (f *kvFSM) get(k string) string { f.mu.Lock(); defer f.mu.Unlock(); return f.st.KV[k] }

type tnode struct {
	*Node
	fsm  *kvFSM
	dir  string
	addr string
	id   string
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startNode(t *testing.T, id, dir, addr, seed string, peers []string) *tnode {
	t.Helper()
	f := newKV(dir)
	n, err := Start(Config{Service: "test", NodeID: id, Advertise: addr, DataDir: dir, FSM: f, Seed: seed, Peers: peers,
		Logf: func(format string, a ...any) { t.Logf(id+": "+format, a...) }})
	if err != nil {
		t.Fatal(err)
	}
	return &tnode{Node: n, fsm: f, dir: dir, addr: addr, id: id}
}

func newNode(t *testing.T, id, seed string, peers []string) *tnode {
	return startNode(t, id, t.TempDir(), freeAddr(t), seed, peers)
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("超时：%s", what)
}

func mustApply(t *testing.T, n *tnode, cmd string) {
	t.Helper()
	var err error
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = n.Apply(ctx, []byte(cmd))
		cancel()
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s apply %q: %v", n.id, cmd, err)
}

func allHave(t *testing.T, nodes []*tnode, k, v string) {
	t.Helper()
	waitFor(t, fmt.Sprintf("所有节点 %s=%s", k, v), 20*time.Second, func() bool {
		for _, n := range nodes {
			if n.fsm.get(k) != v {
				return false
			}
		}
		return true
	})
}

func joinNode(t *testing.T, id string, via *tnode) *tnode {
	t.Helper()
	n := newNode(t, id, via.Seed(), []string{via.addr})
	waitFor(t, id+" 加入", 30*time.Second, func() bool { return !n.Info().Joining })
	return n
}

func TestModesAndTransitions(t *testing.T) {

	a := newNode(t, "a", "", nil)
	defer a.Close()
	if a.Info().Role != "single" {
		t.Fatalf("role = %s", a.Info().Role)
	}
	mustApply(t, a, "x 1")

	// 1 → 2：主备
	b := joinNode(t, "b", a)
	defer b.Close()
	allHave(t, []*tnode{a, b}, "x", "1")
	if a.Info().Role != "primary" || b.Info().Role != "replica" {
		t.Fatalf("roles a=%s b=%s", a.Info().Role, b.Info().Role)
	}
	mustApply(t, b, "y 2") // 备设备写入会转发给主设备
	allHave(t, []*tnode{a, b}, "y", "2")

	// 切换主设备
	if err := b.SetWriter(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "b 成为主设备", 10*time.Second, func() bool { return b.Info().Role == "primary" && a.Info().Role == "replica" })
	mustApply(t, a, "z 3")
	allHave(t, []*tnode{a, b}, "z", "3")

	// 2 → 3：Raft
	c := joinNode(t, "c", a)
	defer c.Close()
	nodes := []*tnode{a, b, c}
	waitFor(t, "三台都进入 raft", 30*time.Second, func() bool {
		for _, n := range nodes {
			if n.Info().Mode != ModeRaft {
				return false
			}
		}
		return true
	})
	allHave(t, nodes, "x", "1")
	mustApply(t, c, "w 4")
	allHave(t, nodes, "w", "4")

	// 3 → 4 → 3
	d := joinNode(t, "d", c)
	nodes = append(nodes, d)
	allHave(t, nodes, "w", "4")
	mustApply(t, d, "v 5")
	allHave(t, nodes, "v", "5")
	if err := a.RemoveMember(context.Background(), "d"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "d 被移除", 20*time.Second, func() bool { return d.Info().Role == "removed" })
	d.Close()
	nodes = nodes[:3]

	// 3 → 2：移除一台非 leader
	var leader, victim *tnode
	waitFor(t, "选出 leader", 20*time.Second, func() bool {
		for _, n := range nodes {
			if n.Info().Role == "leader" {
				leader = n
				return true
			}
		}
		return false
	})
	for _, n := range nodes {
		if n != leader {
			victim = n
			break
		}
	}
	if err := leader.RemoveMember(context.Background(), victim.id); err != nil {
		t.Fatal(err)
	}
	var rest []*tnode
	for _, n := range nodes {
		if n != victim {
			rest = append(rest, n)
		}
	}
	waitFor(t, "剩余两台进入主备", 30*time.Second, func() bool {
		return rest[0].Info().Mode == ModePrimary && rest[1].Info().Mode == ModePrimary
	})
	victim.Close()
	mustApply(t, rest[1], "u 6")
	allHave(t, rest, "u", "6")
	allHave(t, rest, "v", "5")
}

func TestRaftFailover(t *testing.T) {

	a := newNode(t, "a", "", nil)
	b := joinNode(t, "b", a)
	c := joinNode(t, "c", a)
	nodes := []*tnode{a, b, c}
	defer func() {
		for _, n := range nodes {
			n.Close()
		}
	}()
	mustApply(t, b, "k 1")
	allHave(t, nodes, "k", "1")
	var leader *tnode
	waitFor(t, "leader", 20*time.Second, func() bool {
		for _, n := range nodes {
			if n.Info().Role == "leader" {
				leader = n
				return true
			}
		}
		return false
	})
	leader.Close()
	var alive []*tnode
	for _, n := range nodes {
		if n != leader {
			alive = append(alive, n)
		}
	}
	mustApply(t, alive[0], "k 2")
	allHave(t, alive, "k", "2")

	// 原 leader 重启后追上
	re := startNode(t, leader.id, leader.dir, leader.addr, "", nil)
	defer re.Close()
	allHave(t, []*tnode{re}, "k", "2")
}

func TestPrimaryForcePromote(t *testing.T) {

	a := newNode(t, "a", "", nil)
	b := joinNode(t, "b", a)
	defer b.Close()
	mustApply(t, a, "k 1")
	allHave(t, []*tnode{a, b}, "k", "1")
	a.Close()
	if err := b.ForcePromote(); err != nil {
		t.Fatal(err)
	}
	mustApply(t, b, "k 2")
	// 旧主设备恢复后应自动降为备设备并追上
	re := startNode(t, "a", a.dir, a.addr, "", nil)
	defer re.Close()
	waitFor(t, "旧主降级", 20*time.Second, func() bool { return re.Info().Role == "replica" })
	allHave(t, []*tnode{re, b}, "k", "2")
}

func TestMain(m *testing.M) {
	GossipInterval = time.Second
	os.Exit(m.Run())
}

func TestFreshClusterOnExistingData(t *testing.T) {
	dir := t.TempDir()
	f := newKV(dir)
	_, _ = f.Apply(pos(7, 3), []byte("k old"))
	n := startNode(t, "a", dir, freeAddr(t), "", nil)
	defer n.Close()
	if n.Info().Epoch <= 7 {
		t.Fatalf("epoch = %d", n.Info().Epoch)
	}
	mustApply(t, n, "k new")
	if n.fsm.get("k") != "new" || EpochOf(n.fsm.Applied()) <= 7 {
		t.Fatal("新写入未生效")
	}
}

func TestPrimaryAutoPromote(t *testing.T) {
	old, oldReach := AutoPromoteAfter, ReachCheck
	AutoPromoteAfter, ReachCheck = 2*time.Second, func() bool { return true }
	defer func() { AutoPromoteAfter, ReachCheck = old, oldReach }()

	a := newNode(t, "a", "", nil)
	b := joinNode(t, "b", a)
	defer b.Close()
	mustApply(t, a, "k 1")
	allHave(t, []*tnode{a, b}, "k", "1")
	a.Close()
	// 不需要人工操作：备设备在主设备失联后自动成为写节点
	waitFor(t, "备设备自动提升", 30*time.Second, func() bool { return b.IsWriter() })
	mustApply(t, b, "k 2")
	re := startNode(t, "a", a.dir, a.addr, "", nil)
	defer re.Close()
	waitFor(t, "旧主降级", 20*time.Second, func() bool { return re.Info().Role == "replica" })
	allHave(t, []*tnode{re, b}, "k", "2")
}

func TestNoAutoPromoteWhenIsolated(t *testing.T) {
	old, oldReach := AutoPromoteAfter, ReachCheck
	AutoPromoteAfter, ReachCheck = time.Second, func() bool { return false } // 本机自己断网
	defer func() { AutoPromoteAfter, ReachCheck = old, oldReach }()

	a := newNode(t, "a", "", nil)
	b := joinNode(t, "b", a)
	defer b.Close()
	waitFor(t, "b 成为备设备", 20*time.Second, func() bool { return b.Info().Role == "replica" })
	a.Close()
	time.Sleep(6 * time.Second)
	if b.IsWriter() {
		t.Fatal("本机连不上外网时不应自动提升")
	}
}
