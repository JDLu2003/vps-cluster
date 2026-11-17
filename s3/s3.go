// Package s3 是一个无依赖的 S3 兼容对象存储客户端（SigV4），用于 Cloudflare R2 等。
package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// Config 描述一个 S3 兼容的对象存储（Cloudflare R2、MinIO、AWS S3 等）。
type Config struct {
	Endpoint  string // 如 https://<account>.r2.cloudflarestorage.com
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string // R2 用 auto
	Prefix    string // 可选：对象键前缀，如 backup/
}

// Valid 判断配置是否完整。
func (c Config) Valid() bool {
	return c.Endpoint != "" && c.Bucket != "" && c.AccessKey != "" && c.SecretKey != ""
}

type Client struct {
	cfg    Config
	base   *url.URL
	client *http.Client
}

// New 创建客户端。使用 path-style 地址：endpoint/bucket/key。
func New(cfg Config) (*Client, error) {
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")
	if cfg.Region == "" {
		cfg.Region = "auto"
	}
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("S3 配置不完整：需要 endpoint、bucket、access key、secret key")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("S3 endpoint 无效：%q", cfg.Endpoint)
	}
	return &Client{cfg: cfg, base: u, client: &http.Client{Timeout: 120 * time.Second}}, nil
}

func (s *Client) key(k string) string { return s.cfg.Prefix + k }

// uriEncode 按 SigV4 要求编码：除 A-Za-z0-9-._~ 外全部百分号编码；slash 为 false 时连 / 也编码。
func uriEncode(in string, slash bool) string {
	var b strings.Builder
	for _, c := range []byte(in) {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !slash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// do 发起带 SigV4 签名的请求。bodySHA 是请求体的 sha256 十六进制。
func (s *Client) do(method, key string, query url.Values, body []byte, bodySHA string) (*http.Response, error) {
	canonPath := uriEncode(strings.TrimRight(s.base.Path, "/")+"/"+s.cfg.Bucket+"/"+key, false)
	var qs []string
	for k, vs := range query {
		for _, v := range vs {
			qs = append(qs, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	sort.Strings(qs)
	canonQuery := strings.Join(qs, "&")
	u := *s.base
	u.Path, u.RawPath, u.RawQuery = "", "", ""
	target := u.String() + canonPath
	if canonQuery != "" {
		target += "?" + canonQuery
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	day := now.Format("20060102")

	signed := "host;x-amz-content-sha256;x-amz-date"
	canonical := strings.Join([]string{
		method, canonPath, canonQuery,
		"host:" + u.Host + "\nx-amz-content-sha256:" + bodySHA + "\nx-amz-date:" + amzDate + "\n",
		signed, bodySHA,
	}, "\n")
	scope := day + "/" + s.cfg.Region + "/s3/aws4_request"
	ch := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(ch[:])
	k := hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), day)
	k = hmacSHA256(k, s.cfg.Region)
	k = hmacSHA256(k, "s3")
	k = hmacSHA256(k, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(k, toSign))

	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, target, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", bodySHA)
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.cfg.AccessKey, scope, signed, sig))
	return s.client.Do(req)
}

const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func respErr(op string, resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("对象存储 %s 失败：HTTP %d %s", op, resp.StatusCode, strings.TrimSpace(string(b)))
}

// Has 判断对象是否存在。
func (s *Client) Has(key string) bool {
	resp, err := s.do(http.MethodHead, s.key(key), nil, nil, emptySHA)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Put 上传对象。
func (s *Client) Put(key string, data []byte) error {
	sum := sha256.Sum256(data)
	resp, err := s.do(http.MethodPut, s.key(key), nil, data, hex.EncodeToString(sum[:]))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return respErr("PUT", resp)
	}
	return nil
}

// Get 读取对象；不存在时返回 os.ErrNotExist。
func (s *Client) Get(key string) ([]byte, error) {
	resp, err := s.do(http.MethodGet, s.key(key), nil, nil, emptySHA)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}
	if resp.StatusCode != http.StatusOK {
		return nil, respErr("GET", resp)
	}
	return io.ReadAll(resp.Body)
}

// Delete 删除对象（不存在不算错误）。
func (s *Client) Delete(key string) error {
	resp, err := s.do(http.MethodDelete, s.key(key), nil, nil, emptySHA)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return respErr("DELETE", resp)
	}
	return nil
}

// List 列出以 prefix 开头的对象键（返回的键不含配置里的 Prefix）。
func (s *Client) List(prefix string) ([]string, error) {
	var keys []string
	token := ""
	for {
		q := url.Values{"list-type": {"2"}}
		if p := s.cfg.Prefix + prefix; p != "" {
			q.Set("prefix", p)
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := s.do(http.MethodGet, "", q, nil, emptySHA)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			err := respErr("LIST", resp)
			resp.Body.Close()
			return nil, err
		}
		var res struct {
			Contents              []struct{ Key string }
			IsTruncated           bool
			NextContinuationToken string
		}
		err = xml.NewDecoder(resp.Body).Decode(&res)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		for _, c := range res.Contents {
			keys = append(keys, strings.TrimPrefix(c.Key, s.cfg.Prefix))
		}
		if !res.IsTruncated || res.NextContinuationToken == "" {
			return keys, nil
		}
		token = res.NextContinuationToken
	}
}
