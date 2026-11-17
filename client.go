package cluster

import (
	"context"
	"net/http"
	"time"
)

// Client 让持有种子的一方（例如 vpnmgr 面板）管理另一个服务的集群（例如 artifact）。
type Client struct {
	hc *http.Client
}

func NewClient(service, seed string) (*Client, error) {
	id, err := DeriveIdentity(service, seed)
	if err != nil {
		return nil, err
	}
	return &Client{hc: httpClientFor(func() *Identity { return id }, 15*time.Second)}, nil
}

// Info 查询某个节点的状态。
func (c *Client) Info(ctx context.Context, addr string) (*Info, error) {
	var in Info
	if err := doCall(ctx, c.hc, addr, "/_cluster/info", nil, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

// Remove 通过 addr 所在节点（会自动转给写节点）移除成员 id。
func (c *Client) Remove(ctx context.Context, addr, id string) error {
	return doCall(ctx, c.hc, addr, "/_cluster/remove", map[string]string{"id": id}, nil)
}

// SetWriter 通过 addr 所在节点切换写节点。
func (c *Client) SetWriter(ctx context.Context, addr, id string) error {
	return doCall(ctx, c.hc, addr, "/_cluster/writer", map[string]string{"id": id}, nil)
}
