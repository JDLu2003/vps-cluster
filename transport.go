package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// 每个服务只开一个集群端口：TLS 握手后按 ALPN 分流，"vpsc-raft" 交给 Raft，其余交给 HTTP。

const (
	alpnRaft = "vpsc-raft"
	alpnHTTP = "http/1.1"
)

// chanListener 是一个由 accept 循环喂数据的 net.Listener。
type chanListener struct {
	addr   net.Addr
	ch     chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{addr: addr, ch: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) push(c net.Conn) {
	select {
	case l.ch <- c:
	case <-l.closed:
		c.Close()
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *chanListener) Addr() net.Addr { return l.addr }

// mux 持有底层 TCP 监听，并把握手完成的连接分发出去。
type mux struct {
	n    *Node
	ln   net.Listener
	http *chanListener

	mu   sync.Mutex
	raft *chanListener // 只有 Raft 模式下非空
}

func (n *Node) serverTLS() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{alpnRaft, alpnHTTP},
		ClientAuth: tls.RequireAnyClientCert,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			id := n.identity()
			return &id.Cert, nil
		},
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			return n.identity().verify(raw)
		},
	}
}

func (m *mux) serve() {
	for {
		c, err := m.ln.Accept()
		if err != nil {
			return
		}
		go m.handle(c)
	}
}

func (m *mux) handle(c net.Conn) {
	tc := tls.Server(c, m.n.serverTLS())
	_ = tc.SetDeadline(time.Now().Add(10 * time.Second))
	if err := tc.Handshake(); err != nil {
		c.Close()
		return
	}
	_ = tc.SetDeadline(time.Time{})
	if tc.ConnectionState().NegotiatedProtocol == alpnRaft {
		m.mu.Lock()
		l := m.raft
		m.mu.Unlock()
		if l == nil {
			c.Close()
			return
		}
		l.push(tc)
		return
	}
	m.http.push(tc)
}

func (m *mux) newRaftListener() *chanListener {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.raft != nil {
		m.raft.Close()
	}
	m.raft = newChanListener(m.ln.Addr())
	return m.raft
}

func (m *mux) closeRaftListener() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.raft != nil {
		m.raft.Close()
		m.raft = nil
	}
}

// raftStream 实现 raft.StreamLayer。
type raftStream struct {
	*chanListener
	n         *Node
	advertise net.Addr
}

func (s *raftStream) Addr() net.Addr { return s.advertise }

func (s *raftStream) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(d, "tcp", string(addr), s.n.identity().ClientTLS(alpnRaft))
}

type strAddr string

func (a strAddr) Network() string { return "tcp" }
func (a strAddr) String() string  { return string(a) }

// httpClient 返回访问其他节点 HTTP 接口用的客户端（双向 TLS）。
func httpClientFor(id func() *Identity, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: id().ClientTLS(alpnHTTP)}
			return d.DialContext(ctx, network, addr)
		},
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     60 * time.Second,
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}

var errClosed = errors.New("集群节点已关闭")
