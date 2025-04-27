package failover

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// NodeState 节点状态枚举
type NodeState string

const (
	NodeStateUnknown     NodeState = "UNKNOWN"     // 未知状态
	NodeStateUp          NodeState = "UP"          // 正常运行
	NodeStateDown        NodeState = "DOWN"        // 故障状态
	NodeStateDegraded    NodeState = "DEGRADED"    // 性能下降
	NodeStateMaintenance NodeState = "MAINTENANCE" // 维护模式
	NodeStateJoining     NodeState = "JOINING"     // 正在加入集群
	NodeStateLeaving     NodeState = "LEAVING"     // 正在离开集群
)

// Node 集群节点信息
type Node struct {
	ID         string
	Address    string
	Port       int
	Role       string // primary, replica, etc.
	State      NodeState
	LastSeen   time.Time
	CPU        float64 // CPU使用率
	Memory     float64 // 内存使用率
	DiskUsage  float64 // 磁盘使用率
	JoinedTime time.Time
	Metadata   map[string]string
}

// FailoverManager 故障转移管理器
type FailoverManager struct {
	nodes           map[string]*Node
	mutex           sync.RWMutex
	roleManager     *RoleManager
	metrics         *MetricsCollector
	failoverHistory []FailoverEvent
	config          FailoverConfig
}

// FailoverConfig 故障转移配置
type FailoverConfig struct {
	EnableAutoFailover bool
	FailoverTimeout    time.Duration
	MaxFailovers       int           // 单位时间内的最大故障转移次数
	FailoverPeriod     time.Duration // 用于计算最大故障转移的单位时间
	WaitTime           time.Duration // 故障确认等待时间
}

// FailoverEvent 故障转移事件记录
type FailoverEvent struct {
	OldPrimaryID string
	NewPrimaryID string
	Timestamp    time.Time
	Reason       string
	Success      bool
}

// RoleManager 负责节点角色管理（简化版）
type RoleManager struct {
	// 实际实现会更复杂
}

// MetricsCollector 度量收集器（简化版）
type MetricsCollector struct {
	// 实际实现会更复杂
}

// NewFailoverManager 创建新的故障转移管理器
func NewFailoverManager(config FailoverConfig) *FailoverManager {
	// 设置默认配置
	if config.FailoverTimeout == 0 {
		config.FailoverTimeout = 60 * time.Second
	}
	if config.MaxFailovers == 0 {
		config.MaxFailovers = 3
	}
	if config.FailoverPeriod == 0 {
		config.FailoverPeriod = 1 * time.Hour
	}
	if config.WaitTime == 0 {
		config.WaitTime = 5 * time.Second
	}

	return &FailoverManager{
		nodes:           make(map[string]*Node),
		roleManager:     &RoleManager{},
		metrics:         &MetricsCollector{},
		failoverHistory: make([]FailoverEvent, 0),
		config:          config,
	}
}

// RegisterNode 注册新节点
func (fm *FailoverManager) RegisterNode(node *Node) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// 检查节点是否已存在
	if _, exists := fm.nodes[node.ID]; exists {
		return fmt.Errorf("节点 %s 已存在", node.ID)
	}

	// 设置默认值
	if node.State == "" {
		node.State = NodeStateJoining
	}
	if node.JoinedTime.IsZero() {
		node.JoinedTime = time.Now()
	}
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}

	// 存储节点信息
	fm.nodes[node.ID] = node
	log.Printf("注册新节点: %s, 地址: %s:%d, 角色: %s",
		node.ID, node.Address, node.Port, node.Role)

	return nil
}

// UpdateNodeState 更新节点状态
func (fm *FailoverManager) UpdateNodeState(nodeID string, state NodeState) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	node, exists := fm.nodes[nodeID]
	if !exists {
		return fmt.Errorf("节点 %s 不存在", nodeID)
	}

	oldState := node.State
	node.State = state

	log.Printf("节点 %s 状态从 %s 更新为 %s", nodeID, oldState, state)
	return nil
}

// UpdateNodeMetrics 更新节点度量数据
func (fm *FailoverManager) UpdateNodeMetrics(nodeID string, cpu, memory, diskUsage float64) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	node, exists := fm.nodes[nodeID]
	if !exists {
		return fmt.Errorf("节点 %s 不存在", nodeID)
	}

	node.CPU = cpu
	node.Memory = memory
	node.DiskUsage = diskUsage
	node.LastSeen = time.Now()

	return nil
}

// GetNode 获取节点信息
func (fm *FailoverManager) GetNode(nodeID string) (*Node, error) {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	node, exists := fm.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("节点 %s 不存在", nodeID)
	}

	// 返回副本，避免并发修改问题
	nodeCopy := *node
	return &nodeCopy, nil
}

// GetAllNodes 获取所有节点信息
func (fm *FailoverManager) GetAllNodes() map[string]*Node {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	// 创建副本
	nodesCopy := make(map[string]*Node)
	for id, node := range fm.nodes {
		nodeCopy := *node
		nodesCopy[id] = &nodeCopy
	}

	return nodesCopy
}

// RemoveNode 移除节点
func (fm *FailoverManager) RemoveNode(nodeID string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	if _, exists := fm.nodes[nodeID]; !exists {
		return fmt.Errorf("节点 %s 不存在", nodeID)
	}

	delete(fm.nodes, nodeID)
	log.Printf("节点 %s 已移除", nodeID)
	return nil
}

// AutoFailover 执行自动故障转移
func (fm *FailoverManager) AutoFailover(failedNodeID string) error {
	// 检查是否允许自动故障转移
	if !fm.config.EnableAutoFailover {
		return fmt.Errorf("自动故障转移未启用")
	}

	fm.mutex.Lock()
	node, exists := fm.nodes[failedNodeID]
	if !exists {
		fm.mutex.Unlock()
		return fmt.Errorf("节点 %s 不存在", failedNodeID)
	}

	// 检查是否是主节点
	if node.Role != "primary" {
		fm.mutex.Unlock()
		return fmt.Errorf("节点 %s 不是主节点，无需故障转移", failedNodeID)
	}

	// 检查是否超过最大故障转移次数
	recentFailovers := 0
	cutoffTime := time.Now().Add(-fm.config.FailoverPeriod)
	for _, event := range fm.failoverHistory {
		if event.Timestamp.After(cutoffTime) {
			recentFailovers++
		}
	}

	if recentFailovers >= fm.config.MaxFailovers {
		fm.mutex.Unlock()
		return fmt.Errorf("单位时间内故障转移次数已达到上限 (%d/%d)",
			recentFailovers, fm.config.MaxFailovers)
	}

	fm.mutex.Unlock()

	// 等待确认节点确实故障，避免不必要的故障转移
	log.Printf("等待 %v 确认节点 %s 故障...", fm.config.WaitTime, failedNodeID)
	time.Sleep(fm.config.WaitTime)

	// 重新检查节点状态，确认其仍然处于故障状态
	fm.mutex.RLock()
	node, exists = fm.nodes[failedNodeID]
	if !exists || node.State != NodeStateDown {
		fm.mutex.RUnlock()
		return fmt.Errorf("节点 %s 状态已改变，取消故障转移", failedNodeID)
	}
	fm.mutex.RUnlock()

	// 选择新的主节点
	newPrimaryID, err := fm.selectNewPrimary(failedNodeID)
	if err != nil {
		return fmt.Errorf("选择新主节点失败: %v", err)
	}

	// 执行故障转移
	success := fm.executeFailover(failedNodeID, newPrimaryID)

	// 记录故障转移事件
	event := FailoverEvent{
		OldPrimaryID: failedNodeID,
		NewPrimaryID: newPrimaryID,
		Timestamp:    time.Now(),
		Reason:       "自动故障转移 - 节点故障",
		Success:      success,
	}

	fm.mutex.Lock()
	fm.failoverHistory = append(fm.failoverHistory, event)
	fm.mutex.Unlock()

	if !success {
		return fmt.Errorf("故障转移失败")
	}

	log.Printf("节点 %s 的故障转移成功完成，新主节点: %s", failedNodeID, newPrimaryID)
	return nil
}

// selectNewPrimary 选择新的主节点
func (fm *FailoverManager) selectNewPrimary(failedNodeID string) (string, error) {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	var bestCandidate *Node
	var bestScore float64

	// 遍历所有节点，寻找最佳候选者
	for id, node := range fm.nodes {
		// 跳过故障节点和非副本节点
		if id == failedNodeID || node.Role != "replica" || node.State != NodeStateUp {
			continue
		}

		// 计算候选节点分数（简化版）
		// 实际中可能考虑复制延迟、硬件规格、负载等因素
		score := 100.0

		// 根据系统资源使用情况调整分数
		score -= node.CPU * 0.3       // CPU使用率越低越好
		score -= node.Memory * 0.3    // 内存使用率越低越好
		score -= node.DiskUsage * 0.2 // 磁盘使用率越低越好

		// 如果发现更好的候选者，更新记录
		if bestCandidate == nil || score > bestScore {
			bestCandidate = node
			bestScore = score
		}
	}

	if bestCandidate == nil {
		return "", fmt.Errorf("找不到合适的主节点候选者")
	}

	return bestCandidate.ID, nil
}

// executeFailover 执行故障转移操作
func (fm *FailoverManager) executeFailover(oldPrimaryID, newPrimaryID string) bool {
	log.Printf("执行故障转移: 从 %s 到 %s", oldPrimaryID, newPrimaryID)

	// 实际实现中需要进行以下操作：
	// 1. 确保新主节点有所有必要的数据
	// 2. 将新主节点提升为主节点
	// 3. 更新集群配置
	// 4. 通知其他副本连接到新主节点
	// 此处为简化实现

	// 更新节点角色
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	oldPrimary, oldExists := fm.nodes[oldPrimaryID]
	newPrimary, newExists := fm.nodes[newPrimaryID]

	if !oldExists || !newExists {
		log.Printf("故障转移失败: 节点不存在")
		return false
	}

	// 更新角色
	oldPrimary.Role = "failed"
	newPrimary.Role = "primary"
	newPrimary.State = NodeStateUp

	log.Printf("故障转移完成: 节点 %s 现在是主节点", newPrimaryID)
	return true
}

// ManualFailover 执行手动故障转移
func (fm *FailoverManager) ManualFailover(oldPrimaryID, newPrimaryID string) error {
	fm.mutex.RLock()
	_, oldExists := fm.nodes[oldPrimaryID]
	_, newExists := fm.nodes[newPrimaryID]
	fm.mutex.RUnlock()

	if !oldExists {
		return fmt.Errorf("原主节点 %s 不存在", oldPrimaryID)
	}
	if !newExists {
		return fmt.Errorf("新主节点 %s 不存在", newPrimaryID)
	}

	// 执行故障转移
	success := fm.executeFailover(oldPrimaryID, newPrimaryID)

	// 记录故障转移事件
	event := FailoverEvent{
		OldPrimaryID: oldPrimaryID,
		NewPrimaryID: newPrimaryID,
		Timestamp:    time.Now(),
		Reason:       "手动故障转移",
		Success:      success,
	}

	fm.mutex.Lock()
	fm.failoverHistory = append(fm.failoverHistory, event)
	fm.mutex.Unlock()

	if !success {
		return fmt.Errorf("手动故障转移失败")
	}

	return nil
}

// GetFailoverHistory 获取故障转移历史
func (fm *FailoverManager) GetFailoverHistory() []FailoverEvent {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	// 创建副本
	historyCopy := make([]FailoverEvent, len(fm.failoverHistory))
	copy(historyCopy, fm.failoverHistory)

	return historyCopy
}

// SwitchToMaintenanceMode 将节点切换到维护模式
func (fm *FailoverManager) SwitchToMaintenanceMode(nodeID string) error {
	return fm.UpdateNodeState(nodeID, NodeStateMaintenance)
}

// GetNodesByState 按状态获取节点列表
func (fm *FailoverManager) GetNodesByState(state NodeState) []*Node {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	var result []*Node
	for _, node := range fm.nodes {
		if node.State == state {
			nodeCopy := *node
			result = append(result, &nodeCopy)
		}
	}

	return result
}
