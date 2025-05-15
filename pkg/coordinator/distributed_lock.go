package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// CyberDBLock 分布式锁接口
type CyberDBLock interface {
	// 获取锁
	Lock(ctx context.Context) error
	// 释放锁
	Unlock() error
	// 尝试获取锁
	TryLock(ctx context.Context) (bool, error)
	// 是否持有锁
	IsHeldByMe() bool
	// 锁的名称
	Name() string
	// 更新锁TTL
	Refresh(ctx context.Context) error
}

// ConcurrencyLock 基于etcd concurrency的分布式锁实现
type ConcurrencyLock struct {
	// 锁名称
	name string
	// etcd客户端
	client *clientv3.Client
	// etcd会话
	session *concurrency.Session
	// etcd互斥锁
	mutex *concurrency.Mutex
	// 节点ID
	nodeID string
	// 是否已获取锁
	locked bool
	// 互斥锁保护内部状态
	mu sync.Mutex
	// 租约ID
	leaseID clientv3.LeaseID
	// 锁的TTL（秒）
	ttl int64
	// 锁的路径
	path string
}

// NewConcurrencyLock 创建基于etcd的分布式锁
func NewConcurrencyLock(client *clientv3.Client, name string, nodeID string, ttl int64) (*ConcurrencyLock, error) {
	if ttl <= 0 {
		ttl = 10 // 默认10秒
	}

	// 创建会话
	session, err := concurrency.NewSession(client, concurrency.WithTTL(int(ttl)))
	if err != nil {
		return nil, fmt.Errorf("创建etcd会话失败: %w", err)
	}

	// 锁的路径
	path := fmt.Sprintf("/cyberdb/locks/%s", name)

	// 创建互斥锁
	mutex := concurrency.NewMutex(session, path)

	return &ConcurrencyLock{
		name:    name,
		client:  client,
		session: session,
		mutex:   mutex,
		nodeID:  nodeID,
		ttl:     ttl,
		path:    path,
	}, nil
}

// Lock 阻塞获取锁
func (l *ConcurrencyLock) Lock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locked {
		return nil // 已经持有锁
	}

	logger.Debug(fmt.Sprintf("节点 %s 尝试获取锁 %s", l.nodeID, l.name))

	// 获取锁
	if err := l.mutex.Lock(ctx); err != nil {
		return fmt.Errorf("获取etcd锁失败: %w", err)
	}

	l.locked = true
	l.leaseID = l.session.Lease()

	logger.Info(fmt.Sprintf("节点 %s 成功获取锁 %s", l.nodeID, l.name))
	return nil
}

// TryLock 非阻塞尝试获取锁
func (l *ConcurrencyLock) TryLock(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locked {
		return true, nil // 已经持有锁
	}

	// 使用带超时的上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	// 尝试获取锁
	err := l.mutex.Lock(timeoutCtx)
	if err != nil {
		if err == context.DeadlineExceeded {
			// 超时意味着锁被其他人持有
			return false, nil
		}
		return false, fmt.Errorf("尝试获取etcd锁失败: %w", err)
	}

	l.locked = true
	l.leaseID = l.session.Lease()

	logger.Debug(fmt.Sprintf("节点 %s 成功尝试获取锁 %s", l.nodeID, l.name))
	return true, nil
}

// Unlock 释放锁
func (l *ConcurrencyLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.locked {
		return nil // 已经释放锁
	}

	// 释放锁
	if err := l.mutex.Unlock(context.Background()); err != nil {
		return fmt.Errorf("释放etcd锁失败: %w", err)
	}

	l.locked = false
	logger.Debug(fmt.Sprintf("节点 %s 释放锁 %s", l.nodeID, l.name))
	return nil
}

// IsHeldByMe 检查是否由当前节点持有锁
func (l *ConcurrencyLock) IsHeldByMe() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.locked
}

// Name 获取锁名称
func (l *ConcurrencyLock) Name() string {
	return l.name
}

// Refresh 更新锁TTL
func (l *ConcurrencyLock) Refresh(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.locked {
		return fmt.Errorf("未持有锁，无法更新TTL")
	}

	// 使用KeepAliveOnce手动续约
	_, err := l.client.KeepAliveOnce(ctx, l.leaseID)
	if err != nil {
		return fmt.Errorf("更新锁TTL失败: %w", err)
	}

	logger.Debug(fmt.Sprintf("节点 %s 更新锁 %s 的TTL", l.nodeID, l.name))
	return nil
}

// Close 关闭锁并释放资源
func (l *ConcurrencyLock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 如果持有锁，先释放
	if l.locked {
		if err := l.mutex.Unlock(context.Background()); err != nil {
			logger.Error(fmt.Sprintf("关闭时释放锁失败: %v", err))
		}
		l.locked = false
	}

	// 关闭会话
	return l.session.Close()
}

// LockManager 分布式锁管理器
type LockManager struct {
	// etcd客户端
	client *clientv3.Client
	// 节点ID
	nodeID string
	// 活跃锁
	locks sync.Map
	// 锁前缀
	prefix string
}

// NewLockManager 创建分布式锁管理器
func NewLockManager(client *clientv3.Client, nodeID string, prefix string) *LockManager {
	if prefix == "" {
		prefix = "/cyberdb/locks"
	}

	return &LockManager{
		client: client,
		nodeID: nodeID,
		prefix: prefix,
	}
}

// GetLock 获取指定名称的锁
func (lm *LockManager) GetLock(name string, ttl int64) (CyberDBLock, error) {
	// 检查是否已有此锁
	if lock, ok := lm.locks.Load(name); ok {
		return lock.(CyberDBLock), nil
	}

	// 创建新锁
	lock, err := NewConcurrencyLock(lm.client, name, lm.nodeID, ttl)
	if err != nil {
		return nil, err
	}

	// 存储锁
	lm.locks.Store(name, lock)
	return lock, nil
}

// ReleaseLock 释放指定名称的锁
func (lm *LockManager) ReleaseLock(name string) error {
	lockObj, ok := lm.locks.Load(name)
	if !ok {
		return nil // 锁不存在
	}

	lock := lockObj.(*ConcurrencyLock)

	// 释放锁
	if err := lock.Unlock(); err != nil {
		return err
	}

	// 关闭锁
	if err := lock.Close(); err != nil {
		return err
	}

	// 移除锁
	lm.locks.Delete(name)
	return nil
}

// ListActiveLocks 列出活跃的锁
func (lm *LockManager) ListActiveLocks() []string {
	var activeNames []string

	lm.locks.Range(func(key, value interface{}) bool {
		name := key.(string)
		lock := value.(CyberDBLock)
		if lock.IsHeldByMe() {
			activeNames = append(activeNames, name)
		}
		return true
	})

	return activeNames
}

// Close 关闭锁管理器并释放所有锁
func (lm *LockManager) Close() error {
	var lastErr error

	lm.locks.Range(func(key, value interface{}) bool {
		name := key.(string)
		if err := lm.ReleaseLock(name); err != nil {
			logger.Error(fmt.Sprintf("关闭锁管理器时释放锁 %s 失败: %v", name, err))
			lastErr = err
		}
		return true
	})

	return lastErr
}

// AutoRefreshLock 自动刷新锁TTL
func (lm *LockManager) AutoRefreshLock(ctx context.Context, name string, interval time.Duration) error {
	lockObj, ok := lm.locks.Load(name)
	if !ok {
		return fmt.Errorf("锁 %s 不存在", name)
	}

	lock := lockObj.(CyberDBLock)

	// 启动自动刷新协程
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if !lock.IsHeldByMe() {
					return
				}

				if err := lock.Refresh(context.Background()); err != nil {
					logger.Error(fmt.Sprintf("自动刷新锁 %s 失败: %v", name, err))
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// WatchLocks 监视所有锁的变化
func (lm *LockManager) WatchLocks(ctx context.Context) (<-chan LockEvent, error) {
	eventCh := make(chan LockEvent, 100)

	// 监视锁目录
	watchCh := lm.client.Watch(ctx, lm.prefix, clientv3.WithPrefix())

	go func() {
		defer close(eventCh)

		for resp := range watchCh {
			for _, event := range resp.Events {
				var lockEvent LockEvent
				lockEvent.Timestamp = time.Now()

				// 提取锁名称
				key := string(event.Kv.Key)
				lockName := key[len(lm.prefix):]
				lockEvent.Name = lockName

				switch event.Type {
				case clientv3.EventTypePut:
					if event.IsCreate() {
						lockEvent.Type = LockEventAcquired
					} else {
						lockEvent.Type = LockEventRefreshed
					}
				case clientv3.EventTypeDelete:
					lockEvent.Type = LockEventReleased
				}

				select {
				case eventCh <- lockEvent:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return eventCh, nil
}

// LockEvent 锁事件
type LockEvent struct {
	Type      LockEventType
	Name      string
	Timestamp time.Time
}

// LockEventType 锁事件类型
type LockEventType int

const (
	// LockEventAcquired 获取锁事件
	LockEventAcquired LockEventType = iota
	// LockEventReleased 释放锁事件
	LockEventReleased
	// LockEventRefreshed 刷新锁事件
	LockEventRefreshed
)
