package backup

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/JDLu2003/vps-cluster/s3"
	"github.com/JDLu2003/vps-cluster/s3/s3test"
)

type src string

func (s src) Snapshot(w io.Writer) error { _, err := io.WriteString(w, string(s)); return err }

func TestBackupPruneFetch(t *testing.T) {
	f, cfg := s3test.New()
	defer f.Close()
	extra := 0
	r := &Runner{Service: "svc", Source: src("hello"), Keep: 2,
		Config: func() (s3.Config, bool) { return cfg, true },
		Extra:  func(context.Context, *s3.Client) error { extra++; return nil }}
	for i := 0; i < 3; i++ {
		if _, err := r.Now(context.Background()); err != nil {
			t.Fatal(err)
		}
		// 键精确到秒；伪造不同的旧键以测试清理
		f.Objs["svc/snap-2000010"+string(rune('0'+i))+"-00.bin.gz"] = f.Objs[r.Status().LastKey]
	}
	c, _ := s3.New(cfg)
	if err := Prune(c, "svc", 2); err != nil {
		t.Fatal(err)
	}
	keys, _ := List(c, "svc")
	if len(keys) != 2 || extra != 3 {
		t.Fatalf("keys=%v extra=%d", keys, extra)
	}
	rd, _, err := Fetch(c, "svc", "")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rd)
	if string(b) != "hello" {
		t.Fatalf("got %q", b)
	}
}

func TestChangedDebounce(t *testing.T) {
	f, cfg := s3test.New()
	defer f.Close()
	r := &Runner{Service: "svc", Source: src("x"), Debounce: 50 * time.Millisecond, Interval: time.Hour,
		Config: func() (s3.Config, bool) { return cfg, true }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	for i := 0; i < 5; i++ {
		r.Changed()
	}
	deadline := time.Now().Add(3 * time.Second)
	for r.Status().LastKey == "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if r.Status().LastKey == "" {
		t.Fatal("变化后没有触发备份")
	}
}
