package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/config"
	"github.com/cyberdb/cyberdb/pkg/common/logger"
)

// NodeStatus 节点状态
type NodeStatus int

const (
	// NodeStatusUnknown 未知状态
	NodeStatusUnknown NodeStatus = iota
	// NodeStatusOnline 在线状态
	NodeStatusOnline
	// NodeStatusOffline 离线状态
	NodeStatusOffline
	// NodeStatusFailed 故障状态
	NodeStatusFailed
)

// NodeRole 节点角色
type NodeRole int

const (
	// NodeRoleUnknown 未知角色
	NodeRoleUnknown NodeRole = iota
	// NodeRoleLeader 主节点
	NodeRoleLeader
	// NodeRoleFollower 从节点
	NodeRoleFollower
)

// NodeInfo 节点信息
type NodeInfo struct {
	ID       string     // 节点ID
	Address  string     // 节点地址
	Role     NodeRole   // 节点角色
	Status   NodeStatus // 节点状态
	LastSeen time.Time  // 最后一次看到的时间
}

// Manager 集群管理器
type Manager struct {
	// nodeID 本节点ID
	nodeID string

	// role 本节点角色
	role NodeRole

	// addr 本节点地址
	addr string

	// nodes 集群中的所有节点
	nodes map[string]*NodeInfo

	// mu 互斥锁
	mu sync.RWMutex

	// ctx 上下文
	ctx context.Context

	// cancel 取消函数
	cancel context.CancelFunc

	// enabled 是否启用集群
	enabled bool
}

// NewManager 创建新的集群管理器
func NewManager(cfg *config.Config) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		nodeID:  cfg.Server.ID,
		addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.GRPCPort),
		nodes:   make(map[string]*NodeInfo),
		ctx:     ctx,
		cancel:  cancel,
		enabled: cfg.Cluster.Enabled,
	}

	// 如果启用集群，初始化集群节点信息
	if m.enabled {
		// 设置本节点角色
		for _, member := range cfg.Cluster.Members {
			if member.ID == m.nodeID {
				switch member.Role {
				case "leader":
					m.role = NodeRoleLeader
				case "follower":
					m.role = NodeRoleFollower
				default:
					m.role = NodeRoleUnknown
				}
				break
			}
		}

		// 初始化集群节点列表
		for _, member := range cfg.Cluster.Members {
			var role NodeRole
			switch member.Role {
			case "leader":
				role = NodeRoleLeader
			case "follower":
				role = NodeRoleFollower
			default:
				role = NodeRoleUnknown
			}

			m.nodes[member.ID] = &NodeInfo{
				ID:       member.ID,
				Address:  member.Address,
				Role:     role,
				Status:   NodeStatusUnknown,
				LastSeen: time.Time{},
			}
		}
	}

	return m
}

// Start 启动集群管理器
func (m *Manager) Start() error {
	if !m.enabled {
		logger.Info("集群模式未启用，以单节点模式运行")
		return nil
	}

	logger.Info(fmt.Sprintf("启动集群管理器，节点ID: %s, 角色: %s", m.nodeID, m.getRoleName()))

	// 标记本节点为在线状态
	m.mu.Lock()
	if node, ok := m.nodes[m.nodeID]; ok {
		node.Status = NodeStatusOnline
		node.LastSeen = time.Now()
	}
	m.mu.Unlock()

	// 启动心跳检测
	go m.heartbeatLoop()

	return nil
}

// Stop 停止集群管理器
func (m *Manager) Stop() {
	if !m.enabled {
		return
	}

	logger.Info("停止集群管理器")
	m.cancel()
}

// GetNodeStatus 获取节点状态
func (m *Manager) GetNodeStatus(nodeID string) NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if node, ok := m.nodes[nodeID]; ok {
		return node.Status
	}

	return NodeStatusUnknown
}

// GetRole 获取本节点角色
func (m *Manager) GetRole() NodeRole {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.role
}

// IsLeader 检查本节点是否为主节点
func (m *Manager) IsLeader() bool {
	return m.GetRole() == NodeRoleLeader
}

// GetLeader 获取主节点信息
func (m *Manager) GetLeader() *NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, node := range m.nodes {
		if node.Role == NodeRoleLeader {
			return node
		}
	}

	return nil
}

// GetNodes 获取所有节点信息
func (m *Manager) GetNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// UpdateNodeStatus 更新节点状态
func (m *Manager) UpdateNodeStatus(nodeID string, status NodeStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, ok := m.nodes[nodeID]; ok {
		node.Status = status
		node.LastSeen = time.Now()
	}
}

// heartbeatLoop 心跳检测循环
func (m *Manager) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkHeartbeats()
		}
	}
}

// checkHeartbeats 检查节点心跳
func (m *Manager) checkHeartbeats() {
	// 在实际实现中，这里应该向其他节点发送心跳请求
	// 并根据响应更新节点状态
	// 简化版本中，我们只是打印一条日志

	logger.Info("执行集群心跳检查")

	// 模拟心跳检查，更新所有节点状态
	m.mu.Lock()
	for id, node := range m.nodes {
		if id == m.nodeID {
			// 本节点始终在线
			node.Status = NodeStatusOnline
			node.LastSeen = time.Now()
		} else if node.Status != NodeStatusOffline {
			// 模拟检测其他节点，在实际实现中应该发送RPC请求
			// 这里简单随机模拟节点状态
			if time.Since(node.LastSeen) > 15*time.Second {
				node.Status = NodeStatusOffline
				logger.Warn(fmt.Sprintf("节点 %s 离线", node.ID))
			}
		}
	}
	m.mu.Unlock()
}

// getRoleName 获取角色名称
func (m *Manager) getRoleName() string {
	switch m.role {
	case NodeRoleLeader:
		return "leader"
	case NodeRoleFollower:
		return "follower"
	default:
		return "unknown"
	}
}
