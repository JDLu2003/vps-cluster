package cluster

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// 集群身份完全由一个 32 字节的种子（seed）派生：拿到种子就能加入集群、调用内部接口。
// 同一种子按服务名派生出不同的证书，所以 vpnmgr 和 artifact 可以共用一个种子而互不混用。

// NewSeed 生成一个新的随机种子（64 位十六进制）。
func NewSeed() string { return randHex(32) }

// Identity 是某个服务在集群内使用的 TLS 身份：所有节点共用同一张自签名证书，
// 双向 TLS 时要求对方出示的证书与自己完全相同。
type Identity struct {
	Cert tls.Certificate
	Leaf *x509.Certificate
}

// DeriveIdentity 由种子和服务名确定性地生成证书（ed25519 签名是确定性的）。
func DeriveIdentity(service, seed string) (*Identity, error) {
	raw, err := hex.DecodeString(seed)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("集群种子必须是 64 位十六进制")
	}
	h := sha256.Sum256([]byte("vps-cluster/cert/" + service + "/" + string(raw)))
	key := ed25519.NewKeyFromSeed(h[:])
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vps-cluster " + service},
		NotBefore:             time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"vps-cluster"},
	}
	der, err := x509.CreateCertificate(nil, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Identity{Cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, Leaf: leaf}, nil
}

// verify 要求对方证书与本身份逐字节一致。
func (id *Identity) verify(rawCerts [][]byte) error {
	if len(rawCerts) == 0 || string(rawCerts[0]) != string(id.Leaf.Raw) {
		return errors.New("对方不是本集群成员")
	}
	return nil
}

// ClientTLS 返回连接集群节点用的 TLS 配置（会出示本身份证书并校验对方证书）。
func (id *Identity) ClientTLS(alpn string) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{id.Cert},
		InsecureSkipVerify: true, // 不走 CA 链校验，改由 VerifyPeerCertificate 做证书钉扎
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			return id.verify(raw)
		},
		NextProtos: []string{alpn},
		MinVersion: tls.VersionTLS13,
	}
}

// JoinToken 是"加机器"命令里携带的内容。
type JoinToken struct {
	Seed  string   `json:"s"`
	Peers []string `json:"p"` // 现有节点的集群地址 host:port
}

func (t JoinToken) Encode() string {
	b, _ := json.Marshal(t)
	return base64.RawURLEncoding.EncodeToString(b)
}

func ParseJoinToken(s string) (JoinToken, error) {
	var t JoinToken
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return t, fmt.Errorf("加入令牌格式错误: %w", err)
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return t, fmt.Errorf("加入令牌格式错误: %w", err)
	}
	if _, err := DeriveIdentity("check", t.Seed); err != nil {
		return t, err
	}
	return t, nil
}
