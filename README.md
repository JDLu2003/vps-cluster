# vps-cluster

让一个 Go 服务在 1 台、2 台、3 台及以上机器上复制自己的状态，同步方式由机器数自动决定：

| 机器数 | 模式 | 写入 | 写节点宕机 |
| --- | --- | --- | --- |
| 1 | `single`：不复制 | 本机 | — |
| 2 | `primary`：主设备执行写入，命令日志推给备设备（长轮询） | 主设备，可随时切换 | 在备设备上 `ForcePromote`；旧主恢复后自动降级 |
| ≥3 | `raft`（[hashicorp/raft](https://github.com/hashicorp/raft)） | Leader | 自动选主（需多数在线） |

增删成员时由写节点自动切换模式（1→2→3→2→1 均可），切换时状态不丢。

## 用法

服务实现一个确定性的状态机：

```go
type FSM interface {
    Apply(pos uint64, cmd []byte) ([]byte, error) // 无论成功与否都把已应用位置推进到 pos 并持久化
    Applied() uint64
    Snapshot(w io.Writer) error
    Restore(r io.Reader) error
}
```

```go
n, err := cluster.Start(cluster.Config{
    Service: "myapp", NodeID: "a", Advertise: "203.0.113.1:7946", DataDir: "/data/cluster", FSM: fsm,
    Seed: seed, Peers: []string{"203.0.113.2:7946"}, // 加入已有集群时提供
})
out, err := n.Apply(ctx, cmd) // 任何节点都可调用，非写节点自动转发给写节点
```

- `pos = epoch<<40 | 序号`，epoch 在每次模式切换时递增，整个生命周期单调递增；状态机据此跳过重复命令。
- 集群身份由一个 32 字节种子派生（每个服务一张自签名证书，节点间双向 TLS 并钉扎证书）。加入令牌 = 种子 + 现有成员地址。
- 每个服务只用一个端口：TLS 握手后按 ALPN 分给 Raft 或 HTTP（内部接口、日志复制、快照、`/_app/` 服务自定义接口、`/_fwd/` 写请求转发）。
- `backup` 包：写节点定时把快照上传到 S3 兼容存储（Cloudflare R2），只用于备份。
- `s3` 包：无依赖的 SigV4 客户端；`s3/s3test` 是测试用的内存实现。

管理操作：`RemoveMember`、`SetWriter`（主备交换 / Raft 转移 leader）、`ForcePromote`（写节点失联时）、`Status`（并行查询所有成员）。`Client` 让持有种子的另一方管理本集群。

## 开发

```bash
make check   # gofmt + vet + test -race；测试在同一进程内起 1~4 个节点覆盖所有模式与切换
```
