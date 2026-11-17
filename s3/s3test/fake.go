// Package s3test 提供一个内存版的 S3 兼容服务，供测试使用。
package s3test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"

	"github.com/JDLu2003/vps-cluster/s3"
)

type Fake struct {
	*httptest.Server
	mu   sync.Mutex
	Objs map[string][]byte // 键为 bucket 内的对象键
}

// New 启动服务，返回对应的客户端配置（bucket 为 test）。
func New() (*Fake, s3.Config) {
	f := &Fake{Objs: map[string][]byte{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f, s3.Config{Endpoint: f.URL, Bucket: "test", AccessKey: "AK", SecretKey: "SK"}
}

func (f *Fake) Keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ks []string
	for k := range f.Objs {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AK/") {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/test/")
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		type content struct{ Key string }
		res := struct {
			XMLName  xml.Name `xml:"ListBucketResult"`
			Contents []content
		}{}
		p := r.URL.Query().Get("prefix")
		for k := range f.Objs {
			if strings.HasPrefix(k, p) {
				res.Contents = append(res.Contents, content{k})
			}
		}
		_ = xml.NewEncoder(w).Encode(res)
		return
	}
	switch r.Method {
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.Objs[key] = b
	case http.MethodGet, http.MethodHead:
		b, ok := f.Objs[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write(b)
		}
	case http.MethodDelete:
		delete(f.Objs, key)
		w.WriteHeader(http.StatusNoContent)
	}
}
