package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/config"
	"github.com/cyberdb/cyberdb/pkg/common/logger"
	"github.com/cyberdb/cyberdb/pkg/storage"
)

// ReplicationStatus 复制状态
type ReplicationStatus int

const (
	// ReplicationStatusDisabled 禁用
	ReplicationStatusDisabled ReplicationStatus = iota
	// ReplicationStatusInitializing 初始化中
	ReplicationStatusInitializing
	// ReplicationStatusRunning 运行中
	ReplicationStatusRunning
	// ReplicationStatusPaused 暂停
	ReplicationStatusPaused
	// ReplicationStatusError 错误
	ReplicationStatusError
)

// Manager 复制管理器
type Manager struct {
	// nodeID 本节点ID
	nodeID string

	// addr 本节点复制地址
	addr string

	// status 复制状态
	status ReplicationStatus

	// lastError 最后一次错误
	lastError error

	// position 复制位置
	position uint64

	// lastReplicatedAt 最后一次复制时间
	lastReplicatedAt time.Time

	// mu 互斥锁
	mu sync.RWMutex

	// ctx 上下文
	ctx context.Context

	// cancel 取消函数
	cancel context.CancelFunc

	// enabled 是否启用复制
	enabled bool

	// raftReplicator Raft复制器
	raftReplicator *RaftReplicator

	// storageEngine 存储引擎
	storageEngine storage.Engine
}

// NewManager 创建新的复制管理器
func NewManager(cfg *config.Config) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		nodeID:   cfg.Server.ID,
		addr:     fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Replication.Port),
		status:   ReplicationStatusDisabled,
		position: 0,
		ctx:      ctx,
		cancel:   cancel,
		enabled:  cfg.Replication.Enabled,
	}
}

// SetStorageEngine 设置存储引擎
func (m *Manager) SetStorageEngine(engine storage.Engine) {
	m.storageEngine = engine
}

// Start 启动复制管理器
func (m *Manager) Start() error {
	if !m.enabled {
		logger.Info("复制功能未启用")
		return nil
	}

	if m.storageEngine == nil {
		return fmt.Errorf("存储引擎未设置")
	}

	m.mu.Lock()
	m.status = ReplicationStatusInitializing
	m.mu.Unlock()

	logger.Info("启动Raft复制管理器")

	// 创建并启动Raft复制器
	cfg, err := config.GetCurrentConfig()
	if err != nil {
		m.setError(err)
		return err
	}

	m.raftReplicator, err = NewRaftReplicator(cfg, m.storageEngine)
	if err != nil {
		m.setError(err)
		return err
	}

	if err := m.raftReplicator.Start(); err != nil {
		m.setError(err)
		return err
	}

	m.mu.Lock()
	m.status = ReplicationStatusRunning
	m.mu.Unlock()

	// 启动状态检查
	go m.statusCheckLoop()

	return nil
}

// Stop 停止复制管理器
func (m *Manager) Stop() {
	if !m.enabled {
		return
	}

	logger.Info("停止复制管理器")

	m.mu.Lock()
	prevStatus := m.status
	m.status = ReplicationStatusPaused
	m.mu.Unlock()

	// 停止Raft复制器
	if prevStatus == ReplicationStatusRunning && m.raftReplicator != nil {
		if err := m.raftReplicator.Stop(); err != nil {
			logger.Error("停止Raft复制器失败: " + err.Error())
		}
	}

	m.cancel()
}

// GetStatus 获取复制状态
func (m *Manager) GetStatus() ReplicationStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

// GetLastError 获取最后一次错误
func (m *Manager) GetLastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastError
}

// GetPosition 获取复制位置
func (m *Manager) GetPosition() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.position
}

// GetLastReplicatedAt 获取最后一次复制时间
func (m *Manager) GetLastReplicatedAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastReplicatedAt
}

// IsEnabled 检查复制是否启用
func (m *Manager) IsEnabled() bool {
	return m.enabled
}

// IsLeader 检查是否为主节点
func (m *Manager) IsLeader() bool {
	if m.raftReplicator == nil {
		return false
	}
	return m.raftReplicator.IsLeader()
}

// GetLeader 获取主节点地址
func (m *Manager) GetLeader() string {
	if m.raftReplicator == nil {
		return ""
	}
	return m.raftReplicator.GetLeader()
}

// ApplyCommand 应用命令
func (m *Manager) ApplyCommand(cmd *RaftCommand) error {
	if m.raftReplicator == nil {
		return fmt.Errorf("复制器未初始化")
	}

	// 序列化命令
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("序列化命令失败: %w", err)
	}

	// 应用命令
	return m.raftReplicator.ApplyCommand(data)
}

// setError 设置错误状态
func (m *Manager) setError(err error) {
	m.mu.Lock()
	m.status = ReplicationStatusError
	m.lastError = err
	m.mu.Unlock()
	logger.Error("复制错误: " + err.Error())
}

// statusCheckLoop 状态检查循环
func (m *Manager) statusCheckLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkStatus()
		}
	}
}

// checkStatus 检查复制状态
func (m *Manager) checkStatus() {
	if m.raftReplicator == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.status == ReplicationStatusRunning {
		// 更新状态信息
		m.position++
		m.lastReplicatedAt = time.Now()

		isLeader := m.raftReplicator.IsLeader()
		leader := m.raftReplicator.GetLeader()

		role := "follower"
		if isLeader {
			role = "leader"
		}

		logger.Info(fmt.Sprintf("复制状态检查，角色: %s, 主节点: %s, 位置: %d",
			role, leader, m.position))
	}
}
