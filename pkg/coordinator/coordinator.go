package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
	"github.com/cyberdb/cyberdb/pkg/storage"
)

// NodeInfo 节点信息
type NodeInfo struct {
	ID            string            // 节点ID
	Address       string            // 地址（ip:port）
	Role          NodeRole          // 节点角色
	State         NodeState         // 节点状态
	Labels        map[string]string // 节点标签
	Capacity      NodeCapacity      // 节点容量
	StartTime     time.Time         // 启动时间
	LastHeartbeat time.Time         // 最后心跳时间
}

// NodeRole 节点角色
type NodeRole int

const (
	NodeRoleCoordinator NodeRole = iota // 协调节点
	NodeRoleStorage                     // 存储节点
	NodeRoleCompute                     // 计算节点
	NodeRoleFull                        // 全功能节点
)

// NodeState 节点状态
type NodeState int

const (
	NodeStateStarting NodeState = iota // 启动中
	NodeStateRunning                   // 运行中
	NodeStateStopping                  // 停止中
	NodeStateStopped                   // 已停止
	NodeStateFailed                    // 故障
)

// NodeCapacity 节点容量信息
type NodeCapacity struct {
	MaxConnections int64 // 最大连接数
	MaxMemoryMB    int64 // 最大内存（MB）
	MaxStorageGB   int64 // 最大存储（GB）
	MaxCPUCores    int   // 最大CPU核心数
}

// Coordinator 分布式协调器接口
type Coordinator interface {
	// 启动协调器
	Start(ctx context.Context) error

	// 停止协调器
	Stop() error

	// 注册节点
	RegisterNode(ctx context.Context, node *NodeInfo) error

	// 注销节点
	UnregisterNode(ctx context.Context, nodeID string) error

	// 更新节点信息
	UpdateNode(ctx context.Context, node *NodeInfo) error

	// 获取节点信息
	GetNode(ctx context.Context, nodeID string) (*NodeInfo, error)

	// 获取所有节点
	ListNodes(ctx context.Context) ([]*NodeInfo, error)

	// 监听节点变化
	WatchNodes(ctx context.Context) (<-chan NodeEvent, error)

	// 注册分片
	RegisterShard(ctx context.Context, shard *storage.ShardInfo) error

	// 注销分片
	UnregisterShard(ctx context.Context, shardID string) error

	// 获取分片信息
	GetShard(ctx context.Context, shardID string) (*storage.ShardInfo, error)

	// 获取所有分片
	ListShards(ctx context.Context) ([]*storage.ShardInfo, error)

	// 监听分片变化
	WatchShards(ctx context.Context) (<-chan ShardEvent, error)

	// 获取分布式锁
	GetLock(ctx context.Context, name string) (Lock, error)
}

// NodeEvent 节点事件
type NodeEvent struct {
	Type      EventType // 事件类型
	Node      *NodeInfo // 节点信息
	Timestamp time.Time // 事件时间
}

// ShardEvent 分片事件
type ShardEvent struct {
	Type      EventType          // 事件类型
	Shard     *storage.ShardInfo // 分片信息
	Timestamp time.Time          // 事件时间
}

// EventType 事件类型
type EventType int

const (
	EventTypeCreate EventType = iota // 创建
	EventTypeUpdate                  // 更新
	EventTypeDelete                  // 删除
)

// Lock 分布式锁接口
type Lock interface {
	// 获取锁
	Lock(ctx context.Context) error

	// 释放锁
	Unlock() error

	// 刷新锁（延长锁定时间）
	Refresh(ctx context.Context) error
}

// EtcdCoordinator 基于etcd的协调器实现
type EtcdCoordinator struct {
	client   *clientv3.Client // etcd客户端
	prefix   string           // 键前缀
	nodeID   string           // 当前节点ID
	leaseID  clientv3.LeaseID // 租约ID
	leaseTTL int64            // 租约时间（秒）

	heartbeatInterval time.Duration // 心跳间隔
	stopCh            chan struct{} // 停止信号

	mu        sync.RWMutex // 保护内部状态
	isRunning bool         // 是否运行中
}

// NewEtcdCoordinator 创建新的etcd协调器
func NewEtcdCoordinator(endpoints []string, prefix string, nodeID string) (*EtcdCoordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, fmt.Errorf("连接etcd失败: %w", err)
	}

	return &EtcdCoordinator{
		client:            cli,
		prefix:            prefix,
		nodeID:            nodeID,
		leaseTTL:          10, // 默认10秒
		heartbeatInterval: 3 * time.Second,
		stopCh:            make(chan struct{}),
	}, nil
}

// Start 启动协调器
func (e *EtcdCoordinator) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isRunning {
		return nil
	}

	// 创建租约
	resp, err := e.client.Grant(ctx, e.leaseTTL)
	if err != nil {
		return fmt.Errorf("创建租约失败: %w", err)
	}

	e.leaseID = resp.ID

	// 启动心跳协程
	go e.heartbeat(ctx)

	e.isRunning = true
	logger.Info("启动etcd协调器成功")

	return nil
}

// Stop 停止协调器
func (e *EtcdCoordinator) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isRunning {
		return nil
	}

	// 发送停止信号
	close(e.stopCh)

	// 撤销租约
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := e.client.Revoke(ctx, e.leaseID)
	if err != nil {
		logger.Error("撤销租约失败: " + err.Error())
	}

	// 关闭etcd客户端
	err = e.client.Close()
	if err != nil {
		logger.Error("关闭etcd客户端失败: " + err.Error())
	}

	e.isRunning = false
	logger.Info("停止etcd协调器成功")

	return nil
}

// 心跳协程
func (e *EtcdCoordinator) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(e.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 保持租约
			_, err := e.client.KeepAliveOnce(context.Background(), e.leaseID)
			if err != nil {
				logger.Error("保持租约失败: " + err.Error())

				// 尝试重新创建租约
				resp, err := e.client.Grant(context.Background(), e.leaseTTL)
				if err != nil {
					logger.Error("重新创建租约失败: " + err.Error())
					continue
				}

				e.mu.Lock()
				e.leaseID = resp.ID
				e.mu.Unlock()

				logger.Info("成功重新创建租约")
			}

		case <-e.stopCh:
			return

		case <-ctx.Done():
			return
		}
	}
}

// RegisterNode 注册节点
func (e *EtcdCoordinator) RegisterNode(ctx context.Context, node *NodeInfo) error {
	key := fmt.Sprintf("%s/nodes/%s", e.prefix, node.ID)

	// 更新最后心跳时间
	node.LastHeartbeat = time.Now()

	// 序列化节点信息
	value, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("序列化节点信息失败: %w", err)
	}

	// 将节点信息写入etcd，使用租约保证节点故障时自动移除
	_, err = e.client.Put(ctx, key, string(value), clientv3.WithLease(e.leaseID))
	if err != nil {
		return fmt.Errorf("注册节点到etcd失败: %w", err)
	}

	logger.Info(fmt.Sprintf("成功注册节点 %s", node.ID))
	return nil
}

// UnregisterNode 注销节点
func (e *EtcdCoordinator) UnregisterNode(ctx context.Context, nodeID string) error {
	key := fmt.Sprintf("%s/nodes/%s", e.prefix, nodeID)

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("从etcd删除节点失败: %w", err)
	}

	logger.Info(fmt.Sprintf("成功注销节点 %s", nodeID))
	return nil
}

// UpdateNode 更新节点信息
func (e *EtcdCoordinator) UpdateNode(ctx context.Context, node *NodeInfo) error {
	return e.RegisterNode(ctx, node)
}

// GetNode 获取节点信息
func (e *EtcdCoordinator) GetNode(ctx context.Context, nodeID string) (*NodeInfo, error) {
	key := fmt.Sprintf("%s/nodes/%s", e.prefix, nodeID)

	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("从etcd获取节点信息失败: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("节点 %s 不存在", nodeID)
	}

	var node NodeInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &node)
	if err != nil {
		return nil, fmt.Errorf("解析节点信息失败: %w", err)
	}

	return &node, nil
}

// ListNodes 获取所有节点
func (e *EtcdCoordinator) ListNodes(ctx context.Context) ([]*NodeInfo, error) {
	key := fmt.Sprintf("%s/nodes/", e.prefix)

	resp, err := e.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("从etcd获取节点列表失败: %w", err)
	}

	nodes := make([]*NodeInfo, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		var node NodeInfo
		err = json.Unmarshal(kv.Value, &node)
		if err != nil {
			logger.Error(fmt.Sprintf("解析节点信息失败: %s", err.Error()))
			continue
		}

		nodes = append(nodes, &node)
	}

	return nodes, nil
}

// WatchNodes 监听节点变化
func (e *EtcdCoordinator) WatchNodes(ctx context.Context) (<-chan NodeEvent, error) {
	key := fmt.Sprintf("%s/nodes/", e.prefix)
	eventCh := make(chan NodeEvent, 100)

	go func() {
		defer close(eventCh)

		watchCh := e.client.Watch(ctx, key, clientv3.WithPrefix())

		for resp := range watchCh {
			for _, event := range resp.Events {
				var nodeEvent NodeEvent
				nodeEvent.Timestamp = time.Now()

				switch event.Type {
				case clientv3.EventTypePut:
					nodeEvent.Type = EventTypeCreate
					if event.IsModify() {
						nodeEvent.Type = EventTypeUpdate
					}

					var node NodeInfo
					err := json.Unmarshal(event.Kv.Value, &node)
					if err != nil {
						logger.Error(fmt.Sprintf("解析节点信息失败: %s", err.Error()))
						continue
					}

					nodeEvent.Node = &node

				case clientv3.EventTypeDelete:
					nodeEvent.Type = EventTypeDelete

					// 从键中提取节点ID
					nodeID := string(event.Kv.Key)
					nodeID = nodeID[len(key):]

					nodeEvent.Node = &NodeInfo{ID: nodeID}
				}

				select {
				case eventCh <- nodeEvent:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return eventCh, nil
}

// RegisterShard 注册分片
func (e *EtcdCoordinator) RegisterShard(ctx context.Context, shard *storage.ShardInfo) error {
	key := fmt.Sprintf("%s/shards/%s", e.prefix, shard.ID)

	// 序列化分片信息
	value, err := json.Marshal(shard)
	if err != nil {
		return fmt.Errorf("序列化分片信息失败: %w", err)
	}

	// 写入etcd
	_, err = e.client.Put(ctx, key, string(value))
	if err != nil {
		return fmt.Errorf("注册分片到etcd失败: %w", err)
	}

	logger.Info(fmt.Sprintf("成功注册分片 %s", shard.ID))
	return nil
}

// UnregisterShard 注销分片
func (e *EtcdCoordinator) UnregisterShard(ctx context.Context, shardID string) error {
	key := fmt.Sprintf("%s/shards/%s", e.prefix, shardID)

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("从etcd删除分片失败: %w", err)
	}

	logger.Info(fmt.Sprintf("成功注销分片 %s", shardID))
	return nil
}

// GetShard 获取分片信息
func (e *EtcdCoordinator) GetShard(ctx context.Context, shardID string) (*storage.ShardInfo, error) {
	key := fmt.Sprintf("%s/shards/%s", e.prefix, shardID)

	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("从etcd获取分片信息失败: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("分片 %s 不存在", shardID)
	}

	var shard storage.ShardInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &shard)
	if err != nil {
		return nil, fmt.Errorf("解析分片信息失败: %w", err)
	}

	return &shard, nil
}

// ListShards 获取所有分片
func (e *EtcdCoordinator) ListShards(ctx context.Context) ([]*storage.ShardInfo, error) {
	key := fmt.Sprintf("%s/shards/", e.prefix)

	resp, err := e.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("从etcd获取分片列表失败: %w", err)
	}

	shards := make([]*storage.ShardInfo, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		var shard storage.ShardInfo
		err = json.Unmarshal(kv.Value, &shard)
		if err != nil {
			logger.Error(fmt.Sprintf("解析分片信息失败: %s", err.Error()))
			continue
		}

		shards = append(shards, &shard)
	}

	return shards, nil
}

// WatchShards 监听分片变化
func (e *EtcdCoordinator) WatchShards(ctx context.Context) (<-chan ShardEvent, error) {
	key := fmt.Sprintf("%s/shards/", e.prefix)
	eventCh := make(chan ShardEvent, 100)

	go func() {
		defer close(eventCh)

		watchCh := e.client.Watch(ctx, key, clientv3.WithPrefix())

		for resp := range watchCh {
			for _, event := range resp.Events {
				var shardEvent ShardEvent
				shardEvent.Timestamp = time.Now()

				switch event.Type {
				case clientv3.EventTypePut:
					shardEvent.Type = EventTypeCreate
					if event.IsModify() {
						shardEvent.Type = EventTypeUpdate
					}

					var shard storage.ShardInfo
					err := json.Unmarshal(event.Kv.Value, &shard)
					if err != nil {
						logger.Error(fmt.Sprintf("解析分片信息失败: %s", err.Error()))
						continue
					}

					shardEvent.Shard = &shard

				case clientv3.EventTypeDelete:
					shardEvent.Type = EventTypeDelete

					// 从键中提取分片ID
					shardID := string(event.Kv.Key)
					shardID = shardID[len(key):]

					shardEvent.Shard = &storage.ShardInfo{ID: shardID}
				}

				select {
				case eventCh <- shardEvent:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return eventCh, nil
}

// GetLock 获取分布式锁
func (e *EtcdCoordinator) GetLock(ctx context.Context, name string) (Lock, error) {
	key := fmt.Sprintf("%s/locks/%s", e.prefix, name)
	return NewEtcdLock(e.client, key, e.nodeID, 10), nil
}

// EtcdLock 基于etcd的分布式锁
type EtcdLock struct {
	client     *clientv3.Client
	key        string
	value      string
	ttl        int64
	leaseID    clientv3.LeaseID
	cancelFunc context.CancelFunc
	isLocked   bool
	mu         sync.Mutex
}

// NewEtcdLock 创建新的etcd锁
func NewEtcdLock(client *clientv3.Client, key, value string, ttl int64) *EtcdLock {
	return &EtcdLock{
		client: client,
		key:    key,
		value:  value,
		ttl:    ttl,
	}
}

// Lock 获取锁
func (l *EtcdLock) Lock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isLocked {
		return nil
	}

	// 创建租约
	resp, err := l.client.Grant(ctx, l.ttl)
	if err != nil {
		return fmt.Errorf("创建租约失败: %w", err)
	}

	l.leaseID = resp.ID

	// 尝试创建key（如果不存在）
	txnResp, err := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.key), "=", 0)).
		Then(clientv3.OpPut(l.key, l.value, clientv3.WithLease(l.leaseID))).
		Commit()

	if err != nil {
		return fmt.Errorf("获取锁失败: %w", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("锁 %s 已被占用", l.key)
	}

	// 启动自动续约
	keepAliveCtx, cancel := context.WithCancel(context.Background())
	l.cancelFunc = cancel

	keepAliveCh, err := l.client.KeepAlive(keepAliveCtx, l.leaseID)
	if err != nil {
		cancel()
		return fmt.Errorf("设置自动续约失败: %w", err)
	}

	go func() {
		for {
			select {
			case <-keepAliveCtx.Done():
				return
			case resp := <-keepAliveCh:
				if resp == nil {
					// KeepAlive失败
					logger.Error(fmt.Sprintf("锁 %s 的租约续约失败", l.key))
					return
				}
			}
		}
	}()

	l.isLocked = true
	return nil
}

// Unlock 释放锁
func (l *EtcdLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.isLocked {
		return nil
	}

	// 停止自动续约
	if l.cancelFunc != nil {
		l.cancelFunc()
	}

	// 删除锁
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := l.client.Delete(ctx, l.key)
	if err != nil {
		return fmt.Errorf("删除锁失败: %w", err)
	}

	// 撤销租约
	_, err = l.client.Revoke(ctx, l.leaseID)
	if err != nil {
		logger.Error(fmt.Sprintf("撤销锁 %s 的租约失败: %s", l.key, err.Error()))
	}

	l.isLocked = false
	return nil
}

// Refresh 刷新锁
func (l *EtcdLock) Refresh(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.isLocked {
		return fmt.Errorf("锁未获取")
	}

	// 手动续约
	_, err := l.client.KeepAliveOnce(ctx, l.leaseID)
	if err != nil {
		return fmt.Errorf("刷新锁失败: %w", err)
	}

	return nil
}
