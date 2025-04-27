package cluster

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// 错误定义
var (
	ErrNodeNotFound   = errors.New("节点未找到")
	ErrNodeExists     = errors.New("节点已存在")
	ErrClusterFull    = errors.New("集群已满")
	ErrInvalidRequest = errors.New("无效请求")
)

// NodeState 表示节点状态
type NodeState string

const (
	NodeStateUp      NodeState = "up"      // 正常运行
	NodeStateDown    NodeState = "down"    // 不可用
	NodeStateSuspect NodeState = "suspect" // 可疑状态，可能不可用
	NodeStateLeaving NodeState = "leaving" // 正在离开集群
	NodeStateJoining NodeState = "joining" // 正在加入集群
)

// NodeRole 表示节点角色
type NodeRole string

const (
	NodeRolePrimary NodeRole = "primary" // 主节点
	NodeRoleReplica NodeRole = "replica" // 副本节点
	NodeRoleArbiter NodeRole = "arbiter" // 仲裁节点
)

// Node 表示集群中的一个节点
type Node struct {
	ID         string            // 节点ID
	Address    string            // 主机地址
	Port       int               // 端口
	Role       NodeRole          // 角色
	State      NodeState         // 状态
	LastSeen   time.Time         // 最后一次通信时间
	JoinedTime time.Time         // 加入集群时间
	CPU        float64           // CPU使用率
	Memory     float64           // 内存使用率
	DiskUsage  float64           // 磁盘使用率
	Metadata   map[string]string // 元数据
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	NodeID        string        // 当前节点ID
	BindAddr      string        // 绑定地址
	BindPort      int           // 绑定端口
	Seeds         []string      // 种子节点列表
	HeartbeatInt  time.Duration // 心跳间隔
	ProbeTimeout  time.Duration // 探测超时
	SuspectTime   time.Duration // 怀疑时间
	MaxNodes      int           // 最大节点数
	MinPrimaryNum int           // 最小主节点数
}

// Cluster 表示数据库集群
type Cluster struct {
	config     ClusterConfig          // 集群配置
	nodes      map[string]*Node       // 节点列表
	localNode  *Node                  // 当前节点
	nodesMutex sync.RWMutex           // 节点列表互斥锁
	listeners  []ClusterEventListener // 事件监听器
	transport  *rpc.Server            // RPC服务器
	listener   net.Listener           // 网络监听器
	statsMutex sync.Mutex             // 统计信息互斥锁
	metrics    map[string]int64       // 集群指标
	isRunning  bool                   // 是否正在运行
	shutdownCh chan struct{}          // 关闭通道
}

// ClusterEvent 表示集群事件类型
type ClusterEvent string

const (
	NodeJoined    ClusterEvent = "node_joined"    // 节点加入
	NodeLeft      ClusterEvent = "node_left"      // 节点离开
	NodeFailed    ClusterEvent = "node_failed"    // 节点失败
	StateChanged  ClusterEvent = "state_changed"  // 状态变更
	LeaderChanged ClusterEvent = "leader_changed" // 领导者变更
)

// ClusterEventListener 集群事件监听器接口
type ClusterEventListener interface {
	OnClusterEvent(eventType ClusterEvent, node *Node)
}

// NewCluster 创建新的集群实例
func NewCluster(config ClusterConfig) *Cluster {
	return &Cluster{
		config:     config,
		nodes:      make(map[string]*Node),
		metrics:    make(map[string]int64),
		shutdownCh: make(chan struct{}),
		listeners:  make([]ClusterEventListener, 0),
	}
}

// Start 启动集群服务
func (c *Cluster) Start() error {
	if c.isRunning {
		return errors.New("集群已经在运行")
	}

	// 创建本地节点
	c.localNode = &Node{
		ID:         c.config.NodeID,
		Address:    c.config.BindAddr,
		Port:       c.config.BindPort,
		State:      NodeStateJoining,
		JoinedTime: time.Now(),
		LastSeen:   time.Now(),
		Metadata:   make(map[string]string),
	}

	// 添加本地节点到节点列表
	c.nodesMutex.Lock()
	c.nodes[c.config.NodeID] = c.localNode
	c.nodesMutex.Unlock()

	// 启动RPC服务
	if err := c.startRPCServer(); err != nil {
		return fmt.Errorf("启动RPC服务失败: %w", err)
	}

	// 连接种子节点
	if err := c.joinCluster(); err != nil {
		log.Printf("加入集群失败: %v", err)
	}

	// 启动后台任务
	go c.heartbeatLoop()
	go c.failureDetection()

	c.isRunning = true
	log.Printf("集群节点 %s 已启动", c.config.NodeID)

	return nil
}

// Stop 停止集群服务
func (c *Cluster) Stop() error {
	if !c.isRunning {
		return nil
	}

	// 发送停止信号
	close(c.shutdownCh)

	// 通知其他节点我们即将离开
	c.localNode.State = NodeStateLeaving
	c.broadcastNodeState()

	// 关闭网络监听器
	if c.listener != nil {
		c.listener.Close()
	}

	c.isRunning = false
	log.Printf("集群节点 %s 已停止", c.config.NodeID)

	return nil
}

// 启动RPC服务器
func (c *Cluster) startRPCServer() error {
	// 创建RPC服务器
	c.transport = rpc.NewServer()

	// 注册RPC处理程序
	if err := c.transport.Register(c); err != nil {
		return err
	}

	// 启动监听器
	addr := fmt.Sprintf("%s:%d", c.config.BindAddr, c.config.BindPort)
	var err error
	c.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// 处理RPC请求
	go func() {
		for {
			conn, err := c.listener.Accept()
			if err != nil {
				select {
				case <-c.shutdownCh:
					return
				default:
					log.Printf("接受连接错误: %v", err)
				}
				continue
			}
			go c.transport.ServeConn(conn)
		}
	}()

	return nil
}

// 定期发送心跳
func (c *Cluster) heartbeatLoop() {
	ticker := time.NewTicker(c.config.HeartbeatInt)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.broadcastHeartbeat()
		case <-c.shutdownCh:
			return
		}
	}
}

// 发送心跳到所有其他节点
func (c *Cluster) broadcastHeartbeat() {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	for id, node := range c.nodes {
		if id == c.config.NodeID {
			continue // 跳过自己
		}

		if node.State == NodeStateUp || node.State == NodeStateSuspect {
			go c.sendHeartbeat(node)
		}
	}
}

// 发送心跳到指定节点
func (c *Cluster) sendHeartbeat(node *Node) {
	// 更新本地节点状态
	c.localNode.LastSeen = time.Now()

	// 构建心跳请求
	req := HeartbeatRequest{
		Node: *c.localNode,
	}
	resp := HeartbeatResponse{}

	// 调用远程节点的心跳接口
	addr := fmt.Sprintf("%s:%d", node.Address, node.Port)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Printf("连接节点 %s 失败: %v", node.ID, err)
		return
	}
	defer client.Close()

	err = client.Call("Cluster.Heartbeat", req, &resp)
	if err != nil {
		log.Printf("向节点 %s 发送心跳失败: %v", node.ID, err)
		return
	}

	// 处理心跳响应
	c.handleHeartbeatResponse(node.ID, &resp)
}

// HeartbeatRequest 表示心跳请求
type HeartbeatRequest struct {
	Node Node // 当前节点信息
}

// HeartbeatResponse 表示心跳响应
type HeartbeatResponse struct {
	Status  bool             // 状态
	Message string           // 消息
	Nodes   map[string]*Node // 节点列表
}

// Heartbeat 心跳RPC处理
func (c *Cluster) Heartbeat(req *HeartbeatRequest, resp *HeartbeatResponse) error {
	// 更新远程节点信息
	c.nodesMutex.Lock()
	if node, exists := c.nodes[req.Node.ID]; exists {
		node.LastSeen = time.Now()
		node.State = req.Node.State
		node.CPU = req.Node.CPU
		node.Memory = req.Node.Memory
		node.DiskUsage = req.Node.DiskUsage
	} else {
		// 新节点加入
		node := req.Node
		node.LastSeen = time.Now()
		c.nodes[req.Node.ID] = &node

		// 触发节点加入事件
		c.fireEvent(NodeJoined, &node)
	}
	c.nodesMutex.Unlock()

	// 返回集群节点列表
	c.nodesMutex.RLock()
	nodesCopy := make(map[string]*Node, len(c.nodes))
	for id, node := range c.nodes {
		nodeCopy := *node
		nodesCopy[id] = &nodeCopy
	}
	c.nodesMutex.RUnlock()

	resp.Status = true
	resp.Nodes = nodesCopy
	return nil
}

// handleHeartbeatResponse 处理心跳响应
func (c *Cluster) handleHeartbeatResponse(nodeID string, resp *HeartbeatResponse) {
	if !resp.Status {
		log.Printf("心跳响应失败: %s", resp.Message)
		return
	}

	// 更新节点列表
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	for id, node := range resp.Nodes {
		if id == c.config.NodeID {
			continue // 跳过自己
		}

		if existingNode, exists := c.nodes[id]; exists {
			// 更新现有节点
			existingNode.State = node.State
			existingNode.LastSeen = time.Now()
			existingNode.CPU = node.CPU
			existingNode.Memory = node.Memory
			existingNode.DiskUsage = node.DiskUsage
		} else {
			// 添加新节点
			newNode := *node
			newNode.LastSeen = time.Now()
			c.nodes[id] = &newNode

			// 触发节点加入事件
			c.fireEvent(NodeJoined, &newNode)
		}
	}
}

// 故障检测循环
func (c *Cluster) failureDetection() {
	ticker := time.NewTicker(c.config.ProbeTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkNodes()
		case <-c.shutdownCh:
			return
		}
	}
}

// 检查所有节点状态
func (c *Cluster) checkNodes() {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	now := time.Now()
	for id, node := range c.nodes {
		if id == c.config.NodeID {
			continue // 跳过自己
		}

		timeSinceLastSeen := now.Sub(node.LastSeen)

		// 根据上次联系时间确定节点状态
		if node.State != NodeStateDown && node.State != NodeStateLeaving {
			if timeSinceLastSeen > c.config.SuspectTime {
				// 节点超过怀疑时间未联系，标记为Down
				oldState := node.State
				node.State = NodeStateDown

				// 如果状态变化，触发事件
				if oldState != NodeStateDown {
					c.fireEvent(NodeFailed, node)
				}
			} else if timeSinceLastSeen > c.config.ProbeTimeout && node.State != NodeStateSuspect {
				// 节点超过探测超时未联系，标记为怀疑
				oldState := node.State
				node.State = NodeStateSuspect

				// 如果状态变化，触发事件
				if oldState != NodeStateSuspect {
					c.fireEvent(StateChanged, node)
				}
			}
		}
	}
}

// 加入集群
func (c *Cluster) joinCluster() error {
	if len(c.config.Seeds) == 0 {
		// 没有种子节点，作为新集群的第一个节点
		c.localNode.State = NodeStateUp
		c.localNode.Role = NodeRolePrimary
		return nil
	}

	// 尝试连接任一种子节点
	for _, seed := range c.config.Seeds {
		if err := c.joinNode(seed); err == nil {
			c.localNode.State = NodeStateUp
			c.localNode.Role = NodeRoleReplica

			// 成功加入集群
			log.Printf("成功加入集群通过种子节点 %s", seed)
			return nil
		}
	}

	return errors.New("无法连接到任何种子节点")
}

// 加入特定节点
func (c *Cluster) joinNode(addr string) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()

	req := JoinRequest{
		Node: *c.localNode,
	}
	resp := JoinResponse{}

	err = client.Call("Cluster.Join", req, &resp)
	if err != nil {
		return err
	}

	if !resp.Success {
		return errors.New(resp.Message)
	}

	// 更新节点列表
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	for id, node := range resp.Nodes {
		if id == c.config.NodeID {
			continue // 跳过自己
		}
		nodeCopy := *node
		c.nodes[id] = &nodeCopy
	}

	return nil
}

// JoinRequest 表示加入请求
type JoinRequest struct {
	Node Node // 请求加入的节点
}

// JoinResponse 表示加入响应
type JoinResponse struct {
	Success bool             // 是否成功
	Message string           // 消息
	Nodes   map[string]*Node // 节点列表
}

// Join 加入集群RPC处理
func (c *Cluster) Join(req *JoinRequest, resp *JoinResponse) error {
	if len(c.nodes) >= c.config.MaxNodes && c.config.MaxNodes > 0 {
		resp.Success = false
		resp.Message = "集群已达到最大节点数"
		return nil
	}

	// 添加新节点
	c.nodesMutex.Lock()

	if _, exists := c.nodes[req.Node.ID]; exists {
		c.nodesMutex.Unlock()
		resp.Success = false
		resp.Message = "节点ID已存在"
		return nil
	}

	// 默认新加入的节点为副本
	newNode := req.Node
	newNode.Role = NodeRoleReplica
	newNode.State = NodeStateUp
	newNode.LastSeen = time.Now()
	c.nodes[req.Node.ID] = &newNode

	// 复制节点列表
	nodesCopy := make(map[string]*Node, len(c.nodes))
	for id, node := range c.nodes {
		nodeCopy := *node
		nodesCopy[id] = &nodeCopy
	}

	c.nodesMutex.Unlock()

	// 触发节点加入事件
	c.fireEvent(NodeJoined, &newNode)

	// 返回成功
	resp.Success = true
	resp.Nodes = nodesCopy

	return nil
}

// 广播节点状态变更
func (c *Cluster) broadcastNodeState() {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	for id, node := range c.nodes {
		if id == c.config.NodeID {
			continue
		}
		go c.notifyNodeState(node)
	}
}

// 通知节点状态变更
func (c *Cluster) notifyNodeState(node *Node) {
	addr := fmt.Sprintf("%s:%d", node.Address, node.Port)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Printf("连接节点 %s 失败: %v", node.ID, err)
		return
	}
	defer client.Close()

	req := StateChangeRequest{
		NodeID: c.config.NodeID,
		State:  string(c.localNode.State),
	}
	resp := StateChangeResponse{}

	err = client.Call("Cluster.StateChange", req, &resp)
	if err != nil {
		log.Printf("通知节点 %s 状态变更失败: %v", node.ID, err)
	}
}

// StateChangeRequest 表示状态变更请求
type StateChangeRequest struct {
	NodeID string // 节点ID
	State  string // 新状态
}

// StateChangeResponse 表示状态变更响应
type StateChangeResponse struct {
	Success bool   // 是否成功
	Message string // 消息
}

// StateChange 处理状态变更请求
func (c *Cluster) StateChange(req *StateChangeRequest, resp *StateChangeResponse) error {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	if node, exists := c.nodes[req.NodeID]; exists {
		oldState := node.State
		node.State = NodeState(req.State)
		node.LastSeen = time.Now()

		// 根据状态触发相应事件
		switch node.State {
		case NodeStateLeaving:
			c.fireEvent(NodeLeft, node)
		case NodeStateDown:
			c.fireEvent(NodeFailed, node)
		default:
			if oldState != node.State {
				c.fireEvent(StateChanged, node)
			}
		}

		resp.Success = true
	} else {
		resp.Success = false
		resp.Message = "节点不存在"
	}

	return nil
}

// GetNodes 获取集群中的所有节点
func (c *Cluster) GetNodes() []*Node {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}

	return nodes
}

// GetNode 获取特定节点
func (c *Cluster) GetNode(id string) (*Node, error) {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	if node, exists := c.nodes[id]; exists {
		nodeCopy := *node
		return &nodeCopy, nil
	}

	return nil, ErrNodeNotFound
}

// AddListener 添加事件监听器
func (c *Cluster) AddListener(listener ClusterEventListener) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	c.listeners = append(c.listeners, listener)
}

// RemoveListener 移除事件监听器
func (c *Cluster) RemoveListener(listener ClusterEventListener) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	for i, l := range c.listeners {
		if l == listener {
			c.listeners = append(c.listeners[:i], c.listeners[i+1:]...)
			break
		}
	}
}

// 触发事件
func (c *Cluster) fireEvent(eventType ClusterEvent, node *Node) {
	nodeCopy := *node
	for _, listener := range c.listeners {
		go listener.OnClusterEvent(eventType, &nodeCopy)
	}
}

// GetLocalNode 获取本地节点
func (c *Cluster) GetLocalNode() *Node {
	nodeCopy := *c.localNode
	return &nodeCopy
}

// GetPrimaryNodes 获取所有主节点
func (c *Cluster) GetPrimaryNodes() []*Node {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	primaries := make([]*Node, 0)
	for _, node := range c.nodes {
		if node.Role == NodeRolePrimary && node.State == NodeStateUp {
			nodeCopy := *node
			primaries = append(primaries, &nodeCopy)
		}
	}

	return primaries
}

// GetActiveNodeCount 获取活跃节点数量
func (c *Cluster) GetActiveNodeCount() int {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()

	count := 0
	for _, node := range c.nodes {
		if node.State == NodeStateUp {
			count++
		}
	}

	return count
}
