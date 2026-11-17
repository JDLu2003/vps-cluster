// Package backup 把服务的完整快照定时备份到 S3 兼容存储（Cloudflare R2）。
// 只有写节点执行备份；R2 不参与日常读写。
package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JDLu2003/vps-cluster/s3"
)

// Snapshotter 是能写出完整状态的对象（cluster.FSM 满足此接口）。
type Snapshotter interface {
	Snapshot(w io.Writer) error
}

type Runner struct {
	Service  string
	Source   Snapshotter
	IsWriter func() bool
	// Config 返回当前的 R2 配置；ok=false 表示未配置，跳过备份。
	Config   func() (s3.Config, bool)
	Interval time.Duration // 定时备份间隔，默认 6 小时
	Keep     int           // 保留份数，默认 28
	// Debounce > 0 时，Changed() 会在这段时间后触发一次备份（期间多次变化只备份一次），
	// 用于"有写入就尽快备份"。
	Debounce time.Duration
	// Extra 在快照上传后调用，用于增量备份文件等额外内容（可为空）。
	Extra func(ctx context.Context, c *s3.Client) error

	mu      sync.Mutex
	running bool
	status  Status
	kick    chan struct{}
	once    sync.Once
	timer   *time.Timer
}

type Status struct {
	LastAt  int64  `json:"last_at,omitempty"`
	LastKey string `json:"last_key,omitempty"`
	LastErr string `json:"last_err,omitempty"`
	Running bool   `json:"running"`
}

func (r *Runner) init() {
	r.once.Do(func() {
		r.kick = make(chan struct{}, 1)
		if r.Interval <= 0 {
			r.Interval = 6 * time.Hour
		}
		if r.Keep <= 0 {
			r.Keep = 28
		}
	})
}

func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.status
	s.Running = r.running
	return s
}

// Run 阻塞运行，直到 ctx 取消。
func (r *Runner) Run(ctx context.Context) {
	r.init()
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-r.kick:
		}
		if r.IsWriter != nil && !r.IsWriter() {
			continue
		}
		_, _ = r.Now(ctx)
	}
}

// Changed 通知数据有变化：Debounce 之后备份一次（已在等待时不重复安排）。
func (r *Runner) Changed() {
	r.init()
	if r.Debounce <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.timer != nil {
		return
	}
	r.timer = time.AfterFunc(r.Debounce, func() {
		r.mu.Lock()
		r.timer = nil
		r.mu.Unlock()
		r.Trigger()
	})
}

// Trigger 请求后台尽快备份一次。
func (r *Runner) Trigger() {
	r.init()
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// Now 立即备份一次，返回对象键。
func (r *Runner) Now(ctx context.Context) (string, error) {
	r.init()
	cfg, ok := r.Config()
	if !ok {
		return "", fmt.Errorf("尚未配置 R2 备份")
	}
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return "", fmt.Errorf("备份正在进行")
	}
	r.running = true
	r.mu.Unlock()
	key, err := r.do(ctx, cfg)
	r.mu.Lock()
	r.running = false
	r.status.LastAt = time.Now().Unix()
	r.status.LastErr = ""
	if err != nil {
		r.status.LastErr = err.Error()
	} else {
		r.status.LastKey = key
	}
	r.mu.Unlock()
	return key, err
}

func (r *Runner) do(ctx context.Context, cfg s3.Config) (string, error) {
	c, err := s3.New(cfg)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := r.Source.Snapshot(zw); err != nil {
		return "", fmt.Errorf("生成快照: %w", err)
	}
	if err := zw.Close(); err != nil {
		return "", err
	}
	// 精确到小时：同一小时内的多次备份覆盖为一份，保留份数即覆盖的小时数
	key := r.Service + "/snap-" + time.Now().UTC().Format("20060102-15") + ".bin.gz"
	if err := c.Put(key, buf.Bytes()); err != nil {
		return "", err
	}
	if err := Prune(c, r.Service, r.Keep); err != nil {
		return key, fmt.Errorf("快照已上传，但清理旧备份失败: %w", err)
	}
	if r.Extra != nil {
		if err := r.Extra(ctx, c); err != nil {
			return key, fmt.Errorf("快照已上传，但附加备份失败: %w", err)
		}
	}
	return key, nil
}

// List 返回该服务的所有快照键（从旧到新）。
func List(c *s3.Client, service string) ([]string, error) {
	keys, err := c.List(service + "/snap-")
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

// Prune 只保留最新 keep 份。
func Prune(c *s3.Client, service string, keep int) error {
	keys, err := List(c, service)
	if err != nil {
		return err
	}
	for len(keys) > keep {
		if err := c.Delete(keys[0]); err != nil {
			return err
		}
		keys = keys[1:]
	}
	return nil
}

// Fetch 下载并解压一份快照；key 为空时取最新一份。
func Fetch(c *s3.Client, service, key string) (io.Reader, string, error) {
	if key == "" {
		keys, err := List(c, service)
		if err != nil {
			return nil, "", err
		}
		if len(keys) == 0 {
			return nil, "", fmt.Errorf("没有找到 %s 的备份", service)
		}
		key = keys[len(keys)-1]
	}
	b, err := c.Get(key)
	if err != nil {
		return nil, key, err
	}
	if strings.HasSuffix(key, ".gz") {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, key, err
		}
		return zr, key, nil
	}
	return bytes.NewReader(b), key, nil
}
