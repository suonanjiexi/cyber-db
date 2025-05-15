package protocol

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
	"github.com/cyberdb/cyberdb/pkg/compute"
)

var (
	// defaultComputeEngine 默认计算引擎实例
	defaultComputeEngine compute.Engine
)

// SetComputeEngine 设置默认计算引擎
func SetComputeEngine(engine compute.Engine) {
	defaultComputeEngine = engine
}

// MySQLServer 实现MySQL协议服务器
type MySQLServer struct {
	addr          string         // 服务器地址
	connections   int32          // 当前连接数
	listener      net.Listener   // 监听器
	mu            sync.Mutex     // 互斥锁
	running       bool           // 是否正在运行
	computeEngine compute.Engine // 计算引擎
}

// NewMySQLServer 创建新的MySQL协议服务器
func NewMySQLServer(addr string) *MySQLServer {
	return &MySQLServer{
		addr:          addr,
		computeEngine: defaultComputeEngine,
	}
}

// Start 启动MySQL协议服务器
func (s *MySQLServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("服务器已经在运行")
	}

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("监听地址 %s 失败: %w", s.addr, err)
	}

	s.listener = listener
	s.running = true

	go s.acceptLoop()

	return nil
}

// Stop 停止MySQL协议服务器
func (s *MySQLServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	if err := s.listener.Close(); err != nil {
		return fmt.Errorf("关闭监听器失败: %w", err)
	}

	s.running = false

	return nil
}

// ConnectionCount 返回当前连接数
func (s *MySQLServer) ConnectionCount() int {
	return int(atomic.LoadInt32(&s.connections))
}

// acceptLoop 接受新的连接
func (s *MySQLServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// 检查是否是因为服务器关闭导致的错误
			if s.isRunning() {
				logger.Error("接受连接失败: " + err.Error())
			}
			return
		}

		atomic.AddInt32(&s.connections, 1)
		go s.handleConnection(conn)
	}
}

// isRunning 检查服务器是否运行中
func (s *MySQLServer) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// handleConnection 处理单个客户端连接
func (s *MySQLServer) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt32(&s.connections, -1)
	}()

	logger.Info("新的客户端连接: " + conn.RemoteAddr().String())

	// 创建会话
	session := s.computeEngine.CreateSession()
	defer session.Close()

	// 发送初始握手包
	if err := s.sendHandshake(conn); err != nil {
		logger.Error("发送握手包失败: " + err.Error())
		return
	}

	// 验证客户端响应
	if err := s.authenticateClient(conn); err != nil {
		logger.Error("客户端认证失败: " + err.Error())
		return
	}

	// 发送OK包
	if err := s.sendOK(conn); err != nil {
		logger.Error("发送OK包失败: " + err.Error())
		return
	}

	// 处理命令
	for {
		cmd, data, err := s.readCommand(conn)
		if err != nil {
			if err != io.EOF {
				logger.Error("读取命令失败: " + err.Error())
			}
			break
		}

		if err := s.handleCommand(conn, session, cmd, data); err != nil {
			logger.Error("处理命令失败: " + err.Error())
			break
		}
	}
}

// sendHandshake 发送初始握手包
func (s *MySQLServer) sendHandshake(conn net.Conn) error {
	// 这里是简化版的握手包发送
	// 实际实现需要根据MySQL协议格式构造完整的握手包
	handshake := []byte{
		10,                         // 协议版本
		'5', '.', '7', '.', '0', 0, // 服务器版本
		0, 0, 0, 0, // 连接ID
		0, 0, 0, 0, 0, 0, 0, 0, // 认证数据 (简化)
		0,    // 过滤器
		0, 0, // 服务器能力 (低16位)
		0,    // 服务器语言
		0, 0, // 服务器状态
		0, 0, // 服务器能力 (高16位)
		0,                            // 认证数据长度
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 保留字节
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 认证数据2 (简化)
	}

	// 将实际包长度填充到包头
	packetLength := len(handshake)
	header := []byte{
		byte(packetLength),
		byte(packetLength >> 8),
		byte(packetLength >> 16),
		0, // 序列号
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}

	if _, err := conn.Write(handshake); err != nil {
		return err
	}

	return nil
}

// authenticateClient 验证客户端响应
func (s *MySQLServer) authenticateClient(conn net.Conn) error {
	// 读取认证响应
	// 实际实现需要解析客户端发送的认证包并验证用户名和密码
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}

	packetLength := int(header[0]) | int(header[1])<<8 | int(header[2])<<16

	authData := make([]byte, packetLength)
	if _, err := io.ReadFull(conn, authData); err != nil {
		return err
	}

	// 简化的认证逻辑，实际实现需要验证用户名和密码
	// 这里直接返回成功

	return nil
}

// sendOK 发送OK包
func (s *MySQLServer) sendOK(conn net.Conn) error {
	// OK包结构
	ok := []byte{
		0x00,       // OK包标记
		0x00,       // 受影响的行数
		0x00,       // 最后插入的ID
		0x02, 0x00, // 服务器状态
		0x00, 0x00, // 警告数
	}

	// 包头
	header := []byte{
		byte(len(ok)),
		byte(len(ok) >> 8),
		byte(len(ok) >> 16),
		1, // 序列号
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}

	if _, err := conn.Write(ok); err != nil {
		return err
	}

	return nil
}

// readCommand 读取命令
func (s *MySQLServer) readCommand(conn net.Conn) (byte, []byte, error) {
	// 读取包头
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}

	packetLength := int(header[0]) | int(header[1])<<8 | int(header[2])<<16

	// 读取命令数据
	data := make([]byte, packetLength)
	if _, err := io.ReadFull(conn, data); err != nil {
		return 0, nil, err
	}

	// 第一个字节是命令类型
	if len(data) == 0 {
		return 0, nil, fmt.Errorf("空命令")
	}

	return data[0], data[1:], nil
}

// 命令类型常量
const (
	ComQuit  = 0x01
	ComQuery = 0x03
	ComPing  = 0x0e
)

// handleCommand 处理命令
func (s *MySQLServer) handleCommand(conn net.Conn, session *compute.Session, cmd byte, data []byte) error {
	switch cmd {
	case ComQuit:
		// 客户端要求断开连接
		return io.EOF

	case ComPing:
		// 返回OK响应
		return s.sendOK(conn)

	case ComQuery:
		// 执行SQL查询
		query := string(data)
		logger.Info("接收到查询: " + query)

		// 调用计算引擎执行查询
		result, err := session.Execute(query)
		if err != nil {
			return s.sendError(conn, err)
		}

		// 发送结果集
		return s.sendResultSet(conn, result)

	default:
		// 不支持的命令
		return s.sendError(conn, fmt.Errorf("不支持的命令: %d", cmd))
	}
}

// sendError 发送错误包
func (s *MySQLServer) sendError(conn net.Conn, err error) error {
	// 错误包结构
	// 实际实现需要构造符合MySQL协议的错误包

	errMsg := err.Error()
	errPacket := make([]byte, 9+len(errMsg))
	errPacket[0] = 0xff // 错误包标记
	errPacket[1] = 0x00 // 错误码低字节
	errPacket[2] = 0x00 // 错误码高字节
	errPacket[3] = '#'  // SQL状态标记

	// 复制SQL状态(5字节)和错误消息
	copy(errPacket[4:9], "HY000")
	copy(errPacket[9:], errMsg)

	// 包头
	header := []byte{
		byte(len(errPacket)),
		byte(len(errPacket) >> 8),
		byte(len(errPacket) >> 16),
		1, // 序列号
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}

	if _, err := conn.Write(errPacket); err != nil {
		return err
	}

	return nil
}

// sendResultSet 发送结果集
func (s *MySQLServer) sendResultSet(conn net.Conn, result *compute.Result) error {
	// 如果是非查询结果，发送简单的OK包
	if !result.IsQuery {
		// 构造OK包
		ok := []byte{
			0x00, // OK包标记
			byte(result.AffectedRows),
			byte(result.AffectedRows >> 8),
			byte(result.LastInsertID),
			byte(result.LastInsertID >> 8),
			0x02, 0x00, // 服务器状态
			0x00, 0x00, // 警告数
		}

		// 包头
		header := []byte{
			byte(len(ok)),
			byte(len(ok) >> 8),
			byte(len(ok) >> 16),
			1, // 序列号
		}

		if _, err := conn.Write(header); err != nil {
			return err
		}

		if _, err := conn.Write(ok); err != nil {
			return err
		}

		return nil
	}

	// 对于查询结果，需要发送结果集
	// 这里是极度简化的实现，实际情况需要按照MySQL协议构造完整的结果集

	// 简化版的结果集响应
	// 实际实现应该根据MySQL协议的结果集格式发送完整的数据

	// 这里仅作示例，直接返回OK包
	return s.sendOK(conn)
}
