package failover

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// HealthConfig 健康检查配置
type HealthConfig struct {
	CheckInterval      time.Duration // 健康检查间隔
	FailureThreshold   int           // 连续失败次数阈值，触发故障标记
	RecoveryThreshold  int           // 连续成功次数阈值，恢复正常状态
	CPUThreshold       float64       // CPU使用率阈值
	MemoryThreshold    float64       // 内存使用率阈值
	DiskThreshold      float64       // 磁盘使用率阈值
	EnableAutoFailover bool          // 是否启用自动故障转移
}

// NodeHealth 节点健康信息
type NodeHealth struct {
	NodeID        string
	LastCheck     time.Time
	Status        NodeState
	FailureCount  int
	RecoveryCount int
	LastError     string
}

// HealthEvent 健康事件
type HealthEvent struct {
	NodeID    string
	OldStatus NodeState
	NewStatus NodeState
	Timestamp time.Time
	Reason    string
}

// HealthChecker 健康检查器
type HealthChecker struct {
	config          HealthConfig
	failoverManager *FailoverManager
	nodeHealth      map[string]*NodeHealth
	healthEvents    []HealthEvent
	stopChan        chan struct{}
	mutex           sync.RWMutex
}

// NewHealthChecker 创建新的健康检查器
func NewHealthChecker(config HealthConfig, fm *FailoverManager) *HealthChecker {
	// 设置默认值
	if config.CheckInterval == 0 {
		config.CheckInterval = 5 * time.Second
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 3
	}
	if config.RecoveryThreshold == 0 {
		config.RecoveryThreshold = 2
	}
	if config.CPUThreshold == 0 {
		config.CPUThreshold = 90
	}
	if config.MemoryThreshold == 0 {
		config.MemoryThreshold = 85
	}
	if config.DiskThreshold == 0 {
		config.DiskThreshold = 90
	}

	return &HealthChecker{
		config:          config,
		failoverManager: fm,
		nodeHealth:      make(map[string]*NodeHealth),
		healthEvents:    make([]HealthEvent, 0),
		stopChan:        make(chan struct{}),
	}
}

// Start 启动健康检查
func (hc *HealthChecker) Start() {
	go hc.runChecks()
	log.Println("健康检查器已启动")
}

// Stop 停止健康检查
func (hc *HealthChecker) Stop() {
	close(hc.stopChan)
	log.Println("健康检查器已停止")
}

// runChecks 执行定期健康检查
func (hc *HealthChecker) runChecks() {
	ticker := time.NewTicker(hc.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkAllNodes()
		case <-hc.stopChan:
			return
		}
	}
}

// checkAllNodes 检查所有节点的健康状态
func (hc *HealthChecker) checkAllNodes() {
	nodes := hc.failoverManager.GetAllNodes()

	for nodeID, node := range nodes {
		// 对每个节点执行健康检查
		isHealthy := hc.checkNodeHealth(nodeID, node)

		// 更新节点健康状态
		hc.updateNodeHealthStatus(nodeID, isHealthy)
	}
}

// checkNodeHealth 检查单个节点的健康状态
func (hc *HealthChecker) checkNodeHealth(nodeID string, node *Node) bool {
	// 实际环境中，这里应该进行真正的健康检查，如：
	// 1. 发送心跳包
	// 2. 检查节点资源使用情况
	// 3. 检查节点响应时间等

	// 简化示例：检查CPU和内存使用率
	if node.CPU > hc.config.CPUThreshold {
		log.Printf("节点 %s CPU使用率过高: %.2f%%", nodeID, node.CPU)
		return false
	}

	if node.Memory > hc.config.MemoryThreshold {
		log.Printf("节点 %s 内存使用率过高: %.2f%%", nodeID, node.Memory)
		return false
	}

	if node.DiskUsage > hc.config.DiskThreshold {
		log.Printf("节点 %s 磁盘使用率过高: %.2f%%", nodeID, node.DiskUsage)
		return false
	}

	// 检查节点状态
	if node.State == NodeStateDown || node.State == NodeStateMaintenance {
		return false
	}

	return true
}

// updateNodeHealthStatus 更新节点健康状态
func (hc *HealthChecker) updateNodeHealthStatus(nodeID string, isHealthy bool) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	// 获取或创建节点健康记录
	health, exists := hc.nodeHealth[nodeID]
	if !exists {
		health = &NodeHealth{
			NodeID:        nodeID,
			LastCheck:     time.Now(),
			Status:        NodeStateUnknown,
			FailureCount:  0,
			RecoveryCount: 0,
		}
		hc.nodeHealth[nodeID] = health
	}

	oldStatus := health.Status
	health.LastCheck = time.Now()

	if isHealthy {
		// 节点健康
		health.FailureCount = 0
		health.RecoveryCount++

		// 如果连续恢复次数达到阈值，将状态改为UP
		if health.Status != NodeStateUp && health.RecoveryCount >= hc.config.RecoveryThreshold {
			health.Status = NodeStateUp
			log.Printf("节点 %s 已恢复正常状态", nodeID)

			// 记录状态变更事件
			hc.recordHealthEvent(nodeID, oldStatus, health.Status, "节点恢复正常")
		}
	} else {
		// 节点不健康
		health.RecoveryCount = 0
		health.FailureCount++

		// 如果连续失败次数达到阈值，将状态改为DOWN
		if health.Status != NodeStateDown && health.FailureCount >= hc.config.FailureThreshold {
			health.Status = NodeStateDown
			log.Printf("节点 %s 已标记为故障状态", nodeID)

			// 记录状态变更事件
			hc.recordHealthEvent(nodeID, oldStatus, health.Status, "节点故障")

			// 尝试更新远程节点状态
			err := hc.failoverManager.UpdateNodeState(nodeID, NodeStateDown)
			if err != nil {
				log.Printf("更新节点 %s 状态失败: %v", nodeID, err)
			}

			// 如果启用了自动故障转移，则触发
			if hc.config.EnableAutoFailover {
				go hc.triggerAutoFailover(nodeID)
			}
		}
	}
}

// triggerAutoFailover 触发自动故障转移
func (hc *HealthChecker) triggerAutoFailover(nodeID string) {
	log.Printf("为节点 %s 触发自动故障转移", nodeID)

	err := hc.failoverManager.AutoFailover(nodeID)
	if err != nil {
		log.Printf("节点 %s 的自动故障转移失败: %v", nodeID, err)
	} else {
		log.Printf("节点 %s 的自动故障转移成功", nodeID)
	}
}

// recordHealthEvent 记录健康状态变更事件
func (hc *HealthChecker) recordHealthEvent(nodeID string, oldStatus, newStatus NodeState, reason string) {
	event := HealthEvent{
		NodeID:    nodeID,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Timestamp: time.Now(),
		Reason:    reason,
	}

	hc.healthEvents = append(hc.healthEvents, event)

	// 实际应用中可能还需要将事件推送到告警系统
	log.Printf("健康状态变更: 节点 %s 从 %s 变为 %s，原因: %s",
		nodeID, oldStatus, newStatus, reason)
}

// GetNodeHealthStatus 获取节点健康状态
func (hc *HealthChecker) GetNodeHealthStatus(nodeID string) (*NodeHealth, error) {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	health, exists := hc.nodeHealth[nodeID]
	if !exists {
		return nil, fmt.Errorf("节点 %s 的健康记录不存在", nodeID)
	}

	// 返回副本，避免并发修改问题
	return &NodeHealth{
		NodeID:        health.NodeID,
		LastCheck:     health.LastCheck,
		Status:        health.Status,
		FailureCount:  health.FailureCount,
		RecoveryCount: health.RecoveryCount,
		LastError:     health.LastError,
	}, nil
}

// GetAllNodeHealthStatus 获取所有节点的健康状态
func (hc *HealthChecker) GetAllNodeHealthStatus() map[string]*NodeHealth {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	result := make(map[string]*NodeHealth)
	for nodeID, health := range hc.nodeHealth {
		// 返回副本，避免并发修改问题
		result[nodeID] = &NodeHealth{
			NodeID:        health.NodeID,
			LastCheck:     health.LastCheck,
			Status:        health.Status,
			FailureCount:  health.FailureCount,
			RecoveryCount: health.RecoveryCount,
			LastError:     health.LastError,
		}
	}

	return result
}

// GetClusterHealthSummary 获取集群健康摘要
func (hc *HealthChecker) GetClusterHealthSummary() map[string]interface{} {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	totalNodes := len(hc.nodeHealth)
	healthyNodes := 0
	unhealthyNodes := 0
	unknownNodes := 0

	for _, health := range hc.nodeHealth {
		switch health.Status {
		case NodeStateUp:
			healthyNodes++
		case NodeStateDown, NodeStateDegraded:
			unhealthyNodes++
		default:
			unknownNodes++
		}
	}

	// 计算集群整体健康状态
	var clusterStatus string
	healthyRatio := 0.0
	if totalNodes > 0 {
		healthyRatio = float64(healthyNodes) / float64(totalNodes)
	}

	if healthyRatio >= 0.9 {
		clusterStatus = "健康"
	} else if healthyRatio >= 0.7 {
		clusterStatus = "警告"
	} else {
		clusterStatus = "危险"
	}

	return map[string]interface{}{
		"totalNodes":     totalNodes,
		"healthyNodes":   healthyNodes,
		"unhealthyNodes": unhealthyNodes,
		"unknownNodes":   unknownNodes,
		"healthyRatio":   healthyRatio,
		"status":         clusterStatus,
		"timestamp":      time.Now(),
	}
}
