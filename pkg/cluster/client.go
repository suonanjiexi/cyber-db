package cluster

import (
	"context"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// Client 集群客户端，用于连接和管理集群节点
type Client struct {
	connections     map[string]*rpc.Client // 节点ID -> RPC客户端
	connectionMutex sync.RWMutex
	cluster         *Cluster      // 对集群的引用
	reconnectDelay  time.Duration // 重连延迟
	connTimeout     time.Duration // 连接超时
}

// NodeConnection 表示节点连接
type NodeConnection struct {
	client *rpc.Client
	nodeID string
}

// NewClient 创建新的集群客户端
func NewClient(cluster *Cluster) *Client {
	return &Client{
		connections:    make(map[string]*rpc.Client),
		cluster:        cluster,
		reconnectDelay: 5 * time.Second,
		connTimeout:    10 * time.Second,
	}
}

// GetNodeConnection 获取到指定节点的连接
func (c *Client) GetNodeConnection(nodeID string) (*NodeConnection, error) {
	// 首先尝试获取已有连接
	c.connectionMutex.RLock()
	conn, exists := c.connections[nodeID]
	c.connectionMutex.RUnlock()

	if exists {
		return &NodeConnection{client: conn, nodeID: nodeID}, nil
	}

	// 从集群获取节点信息
	node, err := c.cluster.GetNode(nodeID)
	if err != nil {
		return nil, fmt.Errorf("获取节点信息失败: %w", err)
	}

	// 创建新连接
	addr := fmt.Sprintf("%s:%d", node.Address, node.Port)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("连接节点失败: %w", err)
	}

	// 保存连接
	c.connectionMutex.Lock()
	c.connections[nodeID] = client
	c.connectionMutex.Unlock()

	return &NodeConnection{client: client, nodeID: nodeID}, nil
}

// CloseConnections 关闭所有连接
func (c *Client) CloseConnections() {
	c.connectionMutex.Lock()
	defer c.connectionMutex.Unlock()

	for id, conn := range c.connections {
		conn.Close()
		delete(c.connections, id)
	}
}

// Call 在指定节点上调用RPC方法
func (nc *NodeConnection) Call(method string, request interface{}, response interface{}) error {
	return nc.client.Call(method, request, response)
}

// Try 发送Try请求
func (nc *NodeConnection) Try(ctx context.Context, request interface{}) (interface{}, error) {
	response := &struct{ Success bool }{}
	err := nc.client.Call("TCC.Try", request, response)
	return response, err
}

// Confirm 发送Confirm请求
func (nc *NodeConnection) Confirm(ctx context.Context, request interface{}) (interface{}, error) {
	response := &struct{ Success bool }{}
	err := nc.client.Call("TCC.Confirm", request, response)
	return response, err
}

// Cancel 发送Cancel请求
func (nc *NodeConnection) Cancel(ctx context.Context, request interface{}) (interface{}, error) {
	response := &struct{ Success bool }{}
	err := nc.client.Call("TCC.Cancel", request, response)
	return response, err
}

// Close 关闭节点连接
func (nc *NodeConnection) Close() error {
	return nc.client.Close()
}
