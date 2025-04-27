# Cyber-DB 故障转移系统设计文档

## 概述

Cyber-DB 故障转移系统是一个用于检测节点健康状态并在主节点故障时自动进行故障转移的组件。系统由两个主要模块组成：健康检查器和故障转移管理器。

## 核心组件

### 1. 故障转移管理器 (FailoverManager)

故障转移管理器负责维护集群中节点的状态，并在节点故障时执行故障转移操作。

#### 主要功能
- 节点注册和管理
- 节点状态更新
- 自动故障转移
- 手动故障转移
- 故障转移历史记录
- 故障转移策略配置

#### 核心数据结构
- `Node`: 表示集群中的节点，包含ID、地址、角色、状态、资源使用率等信息
- `FailoverEvent`: 记录故障转移事件
- `FailoverConfig`: 配置故障转移行为

### 2. 健康检查器 (HealthChecker)

健康检查器负责监控集群中所有节点的健康状态，并在检测到节点故障时触发故障转移。

#### 主要功能
- 节点健康状态检查
- 健康状态变更记录
- 自动触发故障转移
- 集群健康摘要
- 节点恢复检测

#### 核心数据结构
- `NodeHealth`: 记录节点健康状态
- `HealthEvent`: 记录健康状态变更事件
- `HealthConfig`: 配置健康检查行为

## 工作流程

### 健康检查流程

1. 健康检查器定期检查所有节点的健康状态
2. 检查节点的CPU、内存和磁盘使用率是否超过阈值
3. 如果节点连续失败超过阈值，将节点标记为故障状态
4. 如果节点连续成功超过阈值，将节点标记为正常状态
5. 记录健康状态变更事件
6. 如果启用了自动故障转移，触发故障转移流程

### 故障转移流程

1. 检查是否可以执行故障转移（是否启用自动故障转移，是否超过最大故障转移次数）
2. 等待确认节点确实故障，避免不必要的故障转移
3. 选择一个健康的副本节点作为新的主节点
   - 选择标准包括：节点状态、CPU使用率、内存使用率、磁盘使用率等
4. 将选中的副本节点提升为主节点
5. 更新原主节点和新主节点的角色和状态
6. 记录故障转移事件

## 接口设计

### 故障转移管理器接口

```go
// 创建故障转移管理器
func NewFailoverManager(config FailoverConfig) *FailoverManager

// 注册新节点
func (fm *FailoverManager) RegisterNode(node *Node) error

// 更新节点状态
func (fm *FailoverManager) UpdateNodeState(nodeID string, state NodeState) error

// 更新节点度量数据
func (fm *FailoverManager) UpdateNodeMetrics(nodeID string, cpu, memory, diskUsage float64) error

// 获取节点信息
func (fm *FailoverManager) GetNode(nodeID string) (*Node, error)

// 获取所有节点信息
func (fm *FailoverManager) GetAllNodes() map[string]*Node

// 移除节点
func (fm *FailoverManager) RemoveNode(nodeID string) error

// 执行自动故障转移
func (fm *FailoverManager) AutoFailover(failedNodeID string) error

// 执行手动故障转移
func (fm *FailoverManager) ManualFailover(oldPrimaryID, newPrimaryID string) error

// 获取故障转移历史
func (fm *FailoverManager) GetFailoverHistory() []FailoverEvent
```

### 健康检查器接口

```go
// 创建健康检查器
func NewHealthChecker(config HealthConfig, fm *FailoverManager) *HealthChecker

// 启动健康检查
func (hc *HealthChecker) Start()

// 停止健康检查
func (hc *HealthChecker) Stop()

// 获取节点健康状态
func (hc *HealthChecker) GetNodeHealthStatus(nodeID string) (*NodeHealth, error)

// 获取所有节点的健康状态
func (hc *HealthChecker) GetAllNodeHealthStatus() map[string]*NodeHealth

// 获取集群健康摘要
func (hc *HealthChecker) GetClusterHealthSummary() map[string]interface{}
```

## 配置选项

### 故障转移配置
- `EnableAutoFailover`: 是否启用自动故障转移
- `FailoverTimeout`: 故障转移超时时间
- `MaxFailovers`: 单位时间内的最大故障转移次数
- `FailoverPeriod`: 用于计算最大故障转移的单位时间
- `WaitTime`: 故障确认等待时间

### 健康检查配置
- `CheckInterval`: 健康检查间隔
- `FailureThreshold`: 连续失败次数阈值，触发故障标记
- `RecoveryThreshold`: 连续成功次数阈值，恢复正常状态
- `CPUThreshold`: CPU使用率阈值
- `MemoryThreshold`: 内存使用率阈值
- `DiskThreshold`: 磁盘使用率阈值
- `EnableAutoFailover`: 是否启用自动故障转移

## 故障转移限制

系统实现了故障转移限制机制，防止频繁的故障转移对系统稳定性造成影响：

1. 单位时间内的最大故障转移次数限制
2. 等待时间确认节点确实故障
3. 只有主节点故障才会触发故障转移
4. 必须有可用的健康副本节点才能进行故障转移

## 测试与验证

故障转移系统通过以下测试场景进行验证：

1. 正常操作 - 所有节点正常运行
2. 主节点负载增加 - 主节点CPU/内存/磁盘使用率逐渐增加，直到触发故障转移
3. 主节点故障 - 模拟主节点突然故障的情况

测试结果表明，故障转移系统能够正确检测节点健康状态变化，并在主节点故障时自动选择合适的副本节点进行故障转移。

## 未来改进

1. 添加网络延迟和吞吐量监控
2. 改进副本选择算法，考虑数据同步延迟
3. 添加地理位置感知的故障转移策略
4. 实现分布式共识机制来避免脑裂问题
5. 添加异步复制和同步复制模式的支持
6. 实现自动恢复和重新平衡 