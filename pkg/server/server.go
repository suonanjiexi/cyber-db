package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/parser"
	"github.com/suonanjiexi/cyber-db/pkg/storage"
	"github.com/suonanjiexi/cyber-db/pkg/transaction"
)

// 客户端能力标志
const (
	CLIENT_LONG_PASSWORD                  uint32 = 1
	CLIENT_FOUND_ROWS                     uint32 = 2
	CLIENT_LONG_FLAG                      uint32 = 4
	CLIENT_CONNECT_WITH_DB                uint32 = 8
	CLIENT_NO_SCHEMA                      uint32 = 16
	CLIENT_COMPRESS                       uint32 = 32
	CLIENT_ODBC                           uint32 = 64
	CLIENT_LOCAL_FILES                    uint32 = 128
	CLIENT_IGNORE_SPACE                   uint32 = 256
	CLIENT_PROTOCOL_41                    uint32 = 512
	CLIENT_INTERACTIVE                    uint32 = 1024
	CLIENT_SSL                            uint32 = 2048
	CLIENT_IGNORE_SIGPIPE                 uint32 = 4096
	CLIENT_TRANSACTIONS                   uint32 = 8192
	CLIENT_RESERVED                       uint32 = 16384
	CLIENT_SECURE_CONNECTION              uint32 = 32768
	CLIENT_MULTI_STATEMENTS               uint32 = 65536
	CLIENT_MULTI_RESULTS                  uint32 = 131072
	CLIENT_PS_MULTI_RESULTS               uint32 = 262144
	CLIENT_PLUGIN_AUTH                    uint32 = 524288
	CLIENT_CONNECT_ATTRS                  uint32 = 1048576
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA uint32 = 2097152
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   uint32 = 4194304
	CLIENT_SESSION_TRACK                  uint32 = 8388608
	CLIENT_DEPRECATE_EOF                  uint32 = 16777216
)

// Server 表示数据库服务器
type Server struct {
	host               string
	port               int
	db                 *storage.DB
	txManager          *transaction.Manager
	parser             *parser.Parser
	listener           net.Listener
	mutex              sync.RWMutex
	running            bool
	conns              map[string]net.Conn // 跟踪活跃连接
	connMutex          sync.Mutex          // 连接映射的互斥锁
	charset            string              // 字符集编码
	connTimeout        time.Duration       // 连接超时时间
	queryTimeout       time.Duration       // 查询超时时间
	maxConnections     int                 // 最大连接数
	activeQueries      int32               // 当前活跃查询数
	metrics            map[string]int64    // 性能指标统计
	metricsMutex       sync.Mutex          // 指标互斥锁
	wg                 sync.WaitGroup      // 等待组，用于等待所有连接处理完毕
	nextConnectionID   uint32              // 连接ID生成器
	serverCapabilities uint32              // 服务器能力标志
	serverVersion      string              // 服务器版本
	authPluginName     string              // 认证插件名
	connAuth           map[string][]byte   // 连接认证挑战数据
	connAuthMutex      sync.Mutex          // 认证数据互斥锁
}

// Connection 表示一个客户端连接的状态
type Connection struct {
	ID           uint32            // 连接ID
	Username     string            // 用户名
	Database     string            // 数据库名
	Addr         string            // 客户端地址
	Challenge    []byte            // 认证挑战
	Capabilities uint32            // 客户端能力标志
	Charset      byte              // 字符集
	Status       uint16            // 连接状态
	TxStartTime  time.Time         // 事务开始时间
	LastCmdTime  time.Time         // 最后一次命令时间
	Attrs        map[string]string // 连接属性
	SequenceID   byte              // 当前包序列号
}

// NewServer 创建一个新的数据库服务器
func NewServer(host string, port int, db *storage.DB) *Server {
	txManager := transaction.NewManager(db, 30*time.Second) // 默认30秒超时

	// 服务器能力标志
	capabilities := CLIENT_LONG_PASSWORD |
		CLIENT_FOUND_ROWS |
		CLIENT_LONG_FLAG |
		CLIENT_CONNECT_WITH_DB |
		CLIENT_ODBC |
		CLIENT_IGNORE_SPACE |
		CLIENT_PROTOCOL_41 |
		CLIENT_INTERACTIVE |
		CLIENT_IGNORE_SIGPIPE |
		CLIENT_TRANSACTIONS |
		CLIENT_SECURE_CONNECTION |
		CLIENT_MULTI_STATEMENTS |
		CLIENT_MULTI_RESULTS |
		CLIENT_PS_MULTI_RESULTS |
		CLIENT_PLUGIN_AUTH |
		CLIENT_CONNECT_ATTRS |
		CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA

	return &Server{
		host:               host,
		port:               port,
		db:                 db,
		txManager:          txManager,
		parser:             parser.NewParser(),
		running:            false,
		conns:              make(map[string]net.Conn),
		charset:            "utf8mb4",        // 默认使用utf8mb4字符集
		connTimeout:        5 * time.Minute,  // 默认连接超时时间
		queryTimeout:       30 * time.Second, // 默认查询超时时间
		maxConnections:     100,              // 默认最大连接数
		activeQueries:      0,
		metrics:            make(map[string]int64),
		nextConnectionID:   1,                       // 初始连接ID为1
		serverCapabilities: capabilities,            // 服务器能力标志
		serverVersion:      "8.0.31",                // MySQL 8.0版本
		authPluginName:     "mysql_native_password", // 使用原生密码认证
		connAuth:           make(map[string][]byte), // 存储连接认证挑战数据
	}
}

// SetConnTimeout 设置连接超时时间
func (s *Server) SetConnTimeout(timeout time.Duration) {
	s.connTimeout = timeout
}

// SetQueryTimeout 设置查询超时时间
func (s *Server) SetQueryTimeout(timeout time.Duration) {
	s.queryTimeout = timeout
}

// SetMaxConnections 设置最大连接数
func (s *Server) SetMaxConnections(max int) {
	s.maxConnections = max
}

// Start 启动服务器
func (s *Server) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.running {
		return fmt.Errorf("服务器已经在运行")
	}

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("无法监听 %s: %v", addr, err)
	}

	s.listener = listener
	s.running = true

	log.Printf("服务器正在监听 %s", addr)

	s.wg.Add(1)
	go s.acceptConnections()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return nil
	}

	// 关闭所有活跃连接
	s.connMutex.Lock()
	for id, conn := range s.conns {
		conn.Close()
		delete(s.conns, id)
	}
	s.connMutex.Unlock()

	// 关闭监听器
	if err := s.listener.Close(); err != nil {
		return fmt.Errorf("关闭监听器时出错: %v", err)
	}

	s.running = false
	s.wg.Wait()
	log.Println("服务器已停止")
	return nil
}

// acceptConnections 接受并处理客户端连接
func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running {
				log.Printf("接受连接时出错: %v", err)
			}
			break
		}

		s.connMutex.Lock()
		s.conns[conn.RemoteAddr().String()] = conn
		s.connMutex.Unlock()

		s.wg.Add(1)
		go func(id string, c net.Conn) {
			s.handleConnection(c)

			// 连接处理完毕后移除
			s.connMutex.Lock()
			delete(s.conns, id)
			s.connMutex.Unlock()
		}(conn.RemoteAddr().String(), conn)
	}
}

// handleConnection 处理客户端连接
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	log.Printf("新连接来自 %s", clientAddr)

	// 检查连接数是否超过最大限制
	s.connMutex.Lock()
	connCount := len(s.conns)
	s.connMutex.Unlock()
	if connCount >= s.maxConnections {
		log.Printf("已达到连接限制 (%d)，拒绝来自 %s 的连接", s.maxConnections, conn.RemoteAddr())
		fmt.Fprintf(conn, "错误：服务器连接数已达上限。请稍后再试。\n")
		return
	}

	log.Printf("来自 %s 的新连接（活跃连接：%d/%d）", conn.RemoteAddr(), connCount+1, s.maxConnections)

	// 设置连接超时
	conn.SetDeadline(time.Now().Add(s.connTimeout))

	// 发送MySQL服务器握手包
	if err := s.sendMySQLHandshake(conn); err != nil {
		log.Printf("发送握手包失败: %v", err)
		return
	}

	// 处理客户端认证
	username, err := s.handleClientAuth(conn)
	if err != nil {
		log.Printf("客户端认证失败: %v", err)
		s.sendErrorPacket(conn, 1045, "Access denied")
		return
	}

	log.Printf("客户端 %s 认证成功，用户名: %s", clientAddr, username)

	// 发送认证成功响应
	if err := s.sendOKPacket(conn); err != nil {
		log.Printf("发送OK包失败: %v", err)
		return
	}

	// 创建一个事务
	tx, err := s.txManager.Begin()
	if err != nil {
		log.Printf("为连接 %s 创建事务时出错: %v", conn.RemoteAddr(), err)
		s.sendErrorPacket(conn, 1046, fmt.Sprintf("Error: %v", err))
		return
	}

	// 创建一个上下文，用于处理超时
	ctx, cancel := context.WithTimeout(context.Background(), s.connTimeout)
	defer cancel()

	// 处理客户端命令循环
	for {
		// 重置连接超时
		conn.SetDeadline(time.Now().Add(s.connTimeout))

		// 接收客户端命令
		cmd, err := s.receiveClientCommand(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("接收命令失败: %v", err)
			}
			break
		}

		// 检查是否为退出命令
		if cmd == "quit" || cmd == "exit" {
			// 提交事务并关闭连接
			if err := tx.Commit(); err != nil {
				s.sendErrorPacket(conn, 1046, fmt.Sprintf("Error committing transaction: %v", err))
			}
			break
		}

		// 重置查询超时上下文
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), s.queryTimeout)

		// 解析SQL语句
		stmt, err := s.parser.Parse(cmd)
		if err != nil {
			s.sendErrorPacket(conn, 1064, fmt.Sprintf("Error: %v", err))
			continue
		}

		// 检查事务状态
		txStatus := tx.Status()
		if txStatus != transaction.TxStatusActive {
			// 如果事务不活跃，尝试回滚旧事务
			_ = tx.Rollback()
			// 创建一个新事务
			tx, err = s.txManager.Begin()
			if err != nil {
				s.sendErrorPacket(conn, 1046, fmt.Sprintf("Error creating new transaction: %v", err))
				continue
			}
		}

		// 执行SQL语句（带超时控制）
		resultCh := make(chan string, 1)
		errCh := make(chan error, 1)

		go func() {
			result, err := s.executeStatement(stmt, tx)
			if err != nil {
				errCh <- err
				return
			}
			resultCh <- result
		}()

		// 等待执行结果或超时
		select {
		case result := <-resultCh:
			// 成功执行，发送结果
			s.sendResultPacket(conn, result)
		case err := <-errCh:
			// 执行失败
			s.sendErrorPacket(conn, 1046, fmt.Sprintf("Error: %v", err))
		case <-ctx.Done():
			// 查询超时
			s.sendErrorPacket(conn, 1317, "Query execution was interrupted, timeout exceeded")
		}
	}
}

// 发送MySQL握手包
func (s *Server) sendMySQLHandshake(conn net.Conn) error {
	// 生成随机挑战(auth plugin data)
	challenge := make([]byte, 20)
	if _, err := rand.Read(challenge); err != nil {
		return fmt.Errorf("生成挑战失败: %v", err)
	}

	// 避免0字节，可能导致字符串解析问题
	for i := 0; i < len(challenge); i++ {
		if challenge[i] == 0 {
			challenge[i] = '0'
		}
	}

	// 保存认证挑战，便于后续认证时使用
	conn.(*net.TCPConn).SetNoDelay(true) // 禁用Nagle算法提高响应速度

	// 分配新的连接ID
	connID := atomic.AddUint32(&s.nextConnectionID, 1)

	// 构建MySQL 8.0握手包
	// 参考: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html

	// 首先准备握手包体
	buff := &bytes.Buffer{}

	// 1. 协议版本 (1 byte) - 总是 10
	buff.WriteByte(10)

	// 2. 服务器版本 (NUL结尾)
	buff.WriteString(s.serverVersion)
	buff.WriteByte(0)

	// 3. 线程ID/连接ID (4 bytes)
	buff.WriteByte(byte(connID))
	buff.WriteByte(byte(connID >> 8))
	buff.WriteByte(byte(connID >> 16))
	buff.WriteByte(byte(connID >> 24))

	// 4. 认证挑战的前8字节
	buff.Write(challenge[:8])

	// 5. 填充字节 (1 byte)
	buff.WriteByte(0)

	// 6. 能力标志低16位 (2 bytes)
	capabilityFlags := uint32(
		CLIENT_LONG_PASSWORD |
			CLIENT_FOUND_ROWS |
			CLIENT_LONG_FLAG |
			CLIENT_CONNECT_WITH_DB |
			CLIENT_PLUGIN_AUTH |
			CLIENT_PROTOCOL_41 |
			CLIENT_TRANSACTIONS |
			CLIENT_SECURE_CONNECTION |
			CLIENT_MULTI_STATEMENTS |
			CLIENT_MULTI_RESULTS |
			CLIENT_PS_MULTI_RESULTS |
			CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)

	buff.WriteByte(byte(capabilityFlags))
	buff.WriteByte(byte(capabilityFlags >> 8))

	// 7. 字符集 (1 byte) - utf8mb4_general_ci = 45
	buff.WriteByte(45)

	// 8. 服务器状态 (2 bytes) - 自动提交
	buff.WriteByte(2)
	buff.WriteByte(0)

	// 9. 能力标志高16位 (2 bytes)
	buff.WriteByte(byte(capabilityFlags >> 16))
	buff.WriteByte(byte(capabilityFlags >> 24))

	// 10. 认证数据长度 (1 byte) - 如果有CLIENT_PLUGIN_AUTH，这里必须是21
	buff.WriteByte(21)

	// 11. 保留字节 (10 bytes) - 全部为0
	buff.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	// 12. 认证挑战的剩余部分 (至少12字节) + NULL终止符
	buff.Write(challenge[8:])
	buff.WriteByte(0)

	// 13. 认证插件名称 (NUL结尾)
	buff.WriteString("mysql_native_password")
	buff.WriteByte(0)

	handshake := buff.Bytes()

	// 记录挑战数据到日志，便于调试
	log.Printf("认证挑战: %x", challenge)

	// 构建MySQL包头 (4字节)
	// 长度 (3字节) + 序列号 (1字节)
	packetLen := len(handshake)
	header := []byte{
		byte(packetLen),
		byte(packetLen >> 8),
		byte(packetLen >> 16),
		0, // 包序号 - 握手包总是序号0
	}

	// 发送握手包
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("写入握手包头失败: %v", err)
	}
	if _, err := conn.Write(handshake); err != nil {
		return fmt.Errorf("写入握手包体失败: %v", err)
	}

	log.Printf("发送MySQL 8.0握手包 - 连接ID: %d, 包长度: %d", connID, packetLen)
	return nil
}

// 处理客户端认证
func (s *Server) handleClientAuth(conn net.Conn) (string, error) {
	// 创建连接上下文
	connCtx := &Connection{
		ID:          atomic.LoadUint32(&s.nextConnectionID),
		Addr:        conn.RemoteAddr().String(),
		LastCmdTime: time.Now(),
		Attrs:       make(map[string]string),
		SequenceID:  1, // 预期客户端认证包序列号为1
	}

	// 读取认证包
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", fmt.Errorf("读取认证包头失败: %v", err)
	}

	packetLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	sequenceID := header[3]

	if sequenceID != connCtx.SequenceID {
		log.Printf("警告: 预期序列号 %d，收到 %d", connCtx.SequenceID, sequenceID)
	}

	authData := make([]byte, packetLen)
	if _, err := io.ReadFull(conn, authData); err != nil {
		return "", fmt.Errorf("读取认证包体失败: %v", err)
	}

	// 解析客户端能力标志
	if packetLen < 4 {
		return "", fmt.Errorf("认证包太短")
	}

	clientCapabilities := uint32(authData[0]) |
		uint32(authData[1])<<8 |
		uint32(authData[2])<<16 |
		uint32(authData[3])<<24

	connCtx.Capabilities = clientCapabilities

	// 记录客户端能力标志
	log.Printf("客户端能力标志: 0x%08x", clientCapabilities)

	// 检查客户端是否支持协议41
	if (clientCapabilities & CLIENT_PROTOCOL_41) == 0 {
		return "", fmt.Errorf("客户端不支持必须的协议版本(CLIENT_PROTOCOL_41)")
	}

	// 解析MySQL 8.0认证包
	var pos int = 4 // 能力标志后的位置

	// 最大包大小 (4字节)
	if packetLen < pos+4 {
		return "", fmt.Errorf("无效的认证包: 太短")
	}
	maxPacketSize := uint32(authData[pos]) |
		uint32(authData[pos+1])<<8 |
		uint32(authData[pos+2])<<16 |
		uint32(authData[pos+3])<<24
	pos += 4

	// 字符集 (1字节)
	if packetLen < pos+1 {
		return "", fmt.Errorf("无效的认证包: 太短")
	}
	connCtx.Charset = authData[pos]
	pos += 1

	// 跳过保留字节 (23字节)
	if packetLen < pos+23 {
		return "", fmt.Errorf("无效的认证包: 太短")
	}
	pos += 23

	// 用户名 (NUL结尾)
	usernameEnd := bytes.IndexByte(authData[pos:], 0)
	if usernameEnd == -1 {
		return "", fmt.Errorf("无效的认证包: 未找到用户名终止符")
	}

	connCtx.Username = string(authData[pos : pos+usernameEnd])
	pos += usernameEnd + 1

	log.Printf("客户端认证: 用户=%s, 字符集=%d, 最大包大小=%d",
		connCtx.Username, connCtx.Charset, maxPacketSize)

	// 处理认证响应
	if (clientCapabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
		// 变长长度编码
		if packetLen <= pos {
			return "", fmt.Errorf("无效的认证包: 太短，无法读取认证数据长度")
		}

		authLen := int(authData[pos])
		pos += 1

		if packetLen < pos+authLen {
			return "", fmt.Errorf("无效的认证包: 太短，无法读取完整认证数据")
		}

		// 跳过认证响应数据
		pos += authLen
	} else if (clientCapabilities & CLIENT_SECURE_CONNECTION) != 0 {
		// 固定长度认证响应，前导字节指示长度
		if packetLen <= pos {
			return "", fmt.Errorf("无效的认证包: 太短，无法读取认证数据长度")
		}

		authLen := int(authData[pos])
		pos += 1

		if packetLen < pos+authLen {
			return "", fmt.Errorf("无效的认证包: 太短，无法读取完整认证数据")
		}

		// 跳过认证响应数据
		pos += authLen
	} else {
		// 旧的认证方式 (NUL结尾)
		authEnd := bytes.IndexByte(authData[pos:], 0)
		if authEnd != -1 {
			pos += authEnd + 1
		}
	}

	// 检查数据库名
	if (clientCapabilities&CLIENT_CONNECT_WITH_DB) != 0 && pos < packetLen {
		dbNameEnd := bytes.IndexByte(authData[pos:], 0)
		if dbNameEnd != -1 {
			connCtx.Database = string(authData[pos : pos+dbNameEnd])
			pos += dbNameEnd + 1
		}
	}

	// 检查是否使用了正确的认证插件
	if (clientCapabilities&CLIENT_PLUGIN_AUTH) != 0 && pos < packetLen {
		pluginNameEnd := bytes.IndexByte(authData[pos:], 0)
		if pluginNameEnd != -1 {
			pluginName := string(authData[pos : pos+pluginNameEnd])
			if pluginName != s.authPluginName {
				log.Printf("警告: 客户端请求的插件 '%s' 与服务器期望的 '%s' 不匹配",
					pluginName, s.authPluginName)
			}
		}
	}

	// 在实际系统中，应该验证用户名和密码
	// 为了简化演示，我们接受任何用户

	// 更新包序列号
	connCtx.SequenceID = sequenceID + 1

	// 将认证挑战数据保存到connAuth
	s.connAuthMutex.Lock()
	s.connAuth[conn.RemoteAddr().String()] = challenge
	s.connAuthMutex.Unlock()

	return connCtx.Username, nil
}

// 发送OK包
func (s *Server) sendOKPacket(conn net.Conn) error {
	// OK包格式
	// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html

	// 创建OK包
	buff := &bytes.Buffer{}

	// 包标识: 0x00 表示OK包
	buff.WriteByte(0)

	// 受影响的行数 (使用可变长度编码) - 这里简单使用0
	buff.WriteByte(0)

	// 最后插入ID (使用可变长度编码) - 这里使用0
	buff.WriteByte(0)

	// 服务器状态标志
	serverStatus := uint16(0x0002) // 自动提交标志
	buff.WriteByte(byte(serverStatus))
	buff.WriteByte(byte(serverStatus >> 8))

	// 警告数量
	buff.WriteByte(0)
	buff.WriteByte(0)

	// 带有CLIENT_SESSION_TRACK标志时的额外信息
	// 这里为简化实现，省略

	okPacket := buff.Bytes()

	// 包头
	header := []byte{
		byte(len(okPacket)),
		byte(len(okPacket) >> 8),
		byte(len(okPacket) >> 16),
		2, // 包序号 (认证后的OK包是2)
	}

	// 发送包
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("写入OK包头失败: %v", err)
	}
	if _, err := conn.Write(okPacket); err != nil {
		return fmt.Errorf("写入OK包体失败: %v", err)
	}

	log.Println("发送OK包 - 认证成功")
	return nil
}

// 发送错误包
func (s *Server) sendErrorPacket(conn net.Conn, errCode int, errMsg string) error {
	// 错误包格式
	// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html

	// 创建错误包
	buff := &bytes.Buffer{}

	// 包标识: 0xFF 表示错误包
	buff.WriteByte(0xFF)

	// 错误码 (2字节)
	buff.WriteByte(byte(errCode))
	buff.WriteByte(byte(errCode >> 8))

	// SQL状态标记 '#'
	buff.WriteByte('#')

	// SQL状态 (5字节)
	buff.WriteString("HY000") // 通用错误状态

	// 错误消息
	buff.WriteString(errMsg)

	errPacket := buff.Bytes()

	// 包头
	header := []byte{
		byte(len(errPacket)),
		byte(len(errPacket) >> 8),
		byte(len(errPacket) >> 16),
		2, // 包序号 (认证后的错误包通常是2)
	}

	// 发送包
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("写入错误包头失败: %v", err)
	}
	if _, err := conn.Write(errPacket); err != nil {
		return fmt.Errorf("写入错误包体失败: %v", err)
	}

	log.Printf("发送错误包 - 错误码: %d, 消息: %s", errCode, errMsg)
	return nil
}

// 接收客户端命令
func (s *Server) receiveClientCommand(conn net.Conn) (string, error) {
	// 读取包头
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}

	packetLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16

	// 读取命令包
	cmdPacket := make([]byte, packetLen)
	if _, err := io.ReadFull(conn, cmdPacket); err != nil {
		return "", err
	}

	// 检查包类型
	if len(cmdPacket) == 0 {
		return "", fmt.Errorf("空命令包")
	}

	// COM_QUERY = 3
	if cmdPacket[0] == 3 {
		return string(cmdPacket[1:]), nil
	}

	// COM_QUIT = 1
	if cmdPacket[0] == 1 {
		return "quit", nil
	}

	// 其他命令类型暂不支持
	return "", fmt.Errorf("不支持的命令类型: %d", cmdPacket[0])
}

// 发送结果包
func (s *Server) sendResultPacket(conn net.Conn, result string) error {
	// 将结果字符串拆分为行
	rows := strings.Split(result, "\n")
	if len(rows) == 0 {
		rows = []string{""}
	}

	// 构建自定义列名
	columns := []string{"Result"}

	// 发送结果集
	return s.sendResultSet(conn, columns, rows)
}

// 发送结果集
func (s *Server) sendResultSet(conn net.Conn, columns []string, rows []string) error {
	sequenceID := byte(1) // 结果集的初始序列ID

	// 1. 发送列数
	buff := &bytes.Buffer{}
	// 列数使用变长整数编码
	buff.WriteByte(byte(len(columns)))
	colCountPacket := buff.Bytes()

	// 发送列数包
	header := []byte{
		byte(len(colCountPacket)),
		byte(len(colCountPacket) >> 8),
		byte(len(colCountPacket) >> 16),
		sequenceID,
	}
	sequenceID++

	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("写入列数包头失败: %v", err)
	}
	if _, err := conn.Write(colCountPacket); err != nil {
		return fmt.Errorf("写入列数包体失败: %v", err)
	}

	// 2. 发送列定义
	for _, colName := range columns {
		colDefBuff := &bytes.Buffer{}

		// 目录 "def"
		colDefBuff.WriteByte(3)
		colDefBuff.WriteString("def")

		// 数据库名 (空)
		colDefBuff.WriteByte(0)

		// 表名 (使用"Result")
		tableName := "Result"
		colDefBuff.WriteByte(byte(len(tableName)))
		colDefBuff.WriteString(tableName)

		// 原始表名
		colDefBuff.WriteByte(byte(len(tableName)))
		colDefBuff.WriteString(tableName)

		// 列名
		colDefBuff.WriteByte(byte(len(colName)))
		colDefBuff.WriteString(colName)

		// 原始列名
		colDefBuff.WriteByte(byte(len(colName)))
		colDefBuff.WriteString(colName)

		// 下面是固定部分
		// 字符集(utf8mb4_general_ci), 字段长度(255), 字段类型(VAR_STRING), 标志位等
		colDefBuff.WriteByte(0x0C) // 字符集长度
		colDefBuff.WriteByte(0x2d) // 字符集 (utf8mb4_general_ci = 45 = 0x2d)
		colDefBuff.WriteByte(0x00)
		colDefBuff.WriteByte(0xff) // 列长度 (255)
		colDefBuff.WriteByte(0x00)
		colDefBuff.WriteByte(0x00)
		colDefBuff.WriteByte(0x00)
		colDefBuff.WriteByte(0xfd) // 列类型 (VAR_STRING = 253 = 0xfd)
		colDefBuff.WriteByte(0x00) // 标志 (0)
		colDefBuff.WriteByte(0x00)
		colDefBuff.WriteByte(0x00) // 小数点位数 (0)
		colDefBuff.WriteByte(0x00) // 未使用
		colDefBuff.WriteByte(0x00)

		colDefPacket := colDefBuff.Bytes()

		// 发送列定义
		colHeader := []byte{
			byte(len(colDefPacket)),
			byte(len(colDefPacket) >> 8),
			byte(len(colDefPacket) >> 16),
			sequenceID,
		}
		sequenceID++

		if _, err := conn.Write(colHeader); err != nil {
			return fmt.Errorf("写入列定义包头失败: %v", err)
		}
		if _, err := conn.Write(colDefPacket); err != nil {
			return fmt.Errorf("写入列定义包体失败: %v", err)
		}
	}

	// 3. 发送EOF
	eofBuff := &bytes.Buffer{}
	eofBuff.WriteByte(0xFE) // EOF标识
	eofBuff.WriteByte(0x00) // 警告数
	eofBuff.WriteByte(0x00)
	eofBuff.WriteByte(0x02) // 状态标志 (0x0002 = SERVER_STATUS_AUTOCOMMIT)
	eofBuff.WriteByte(0x00)
	eofPacket := eofBuff.Bytes()

	eofHeader := []byte{
		byte(len(eofPacket)),
		byte(len(eofPacket) >> 8),
		byte(len(eofPacket) >> 16),
		sequenceID,
	}
	sequenceID++

	if _, err := conn.Write(eofHeader); err != nil {
		return fmt.Errorf("写入EOF包头失败: %v", err)
	}
	if _, err := conn.Write(eofPacket); err != nil {
		return fmt.Errorf("写入EOF包体失败: %v", err)
	}

	// 4. 发送数据行
	for _, row := range rows {
		if row == "" {
			continue // 跳过空行
		}

		rowBuff := &bytes.Buffer{}

		// 每列的数据用长度前缀
		rowBuff.WriteByte(byte(len(row)))
		rowBuff.WriteString(row)

		rowPacket := rowBuff.Bytes()

		rowHeader := []byte{
			byte(len(rowPacket)),
			byte(len(rowPacket) >> 8),
			byte(len(rowPacket) >> 16),
			sequenceID,
		}
		sequenceID++

		if _, err := conn.Write(rowHeader); err != nil {
			return fmt.Errorf("写入数据行包头失败: %v", err)
		}
		if _, err := conn.Write(rowPacket); err != nil {
			return fmt.Errorf("写入数据行包体失败: %v", err)
		}
	}

	// 5. 发送结束EOF
	if _, err := conn.Write([]byte{
		byte(len(eofPacket)),
		byte(len(eofPacket) >> 8),
		byte(len(eofPacket) >> 16),
		sequenceID,
	}); err != nil {
		return fmt.Errorf("写入结束EOF包头失败: %v", err)
	}
	if _, err := conn.Write(eofPacket); err != nil {
		return fmt.Errorf("写入结束EOF包体失败: %v", err)
	}

	log.Printf("发送完成结果集 - %d列 %d行", len(columns), len(rows))
	return nil
}

// executeStatement 执行SQL语句
func (s *Server) executeStatement(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	atomic.AddInt32(&s.activeQueries, 1)
	defer atomic.AddInt32(&s.activeQueries, -1)

	// 更新指标
	s.updateMetric("active_queries", int64(atomic.LoadInt32(&s.activeQueries)))
	s.updateMetric("total_queries", 1)

	// 根据语句类型执行不同的操作
	switch stmt.Type {
	case parser.SelectStmt:
		result, err := s.executeSelect(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("select_queries", 1)
		return result, nil

	case parser.InsertStmt:
		result, err := s.executeInsert(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("insert_queries", 1)
		return result, nil

	case parser.UpdateStmt:
		result, err := s.executeUpdate(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("update_queries", 1)
		return result, nil

	case parser.DeleteStmt:
		result, err := s.executeDelete(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("delete_queries", 1)
		return result, nil

	case parser.CreateTableStmt:
		result, err := s.executeCreateTable(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("ddl_queries", 1)
		return result, nil

	case parser.CreateIndexStmt:
		result, err := s.executeCreateIndex(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("ddl_queries", 1)
		return result, nil

	case parser.DropIndexStmt:
		result, err := s.executeDropIndex(stmt, tx)
		if err != nil {
			s.updateMetric("failed_queries", 1)
			return "", err
		}
		s.updateMetric("ddl_queries", 1)
		return result, nil

	default:
		s.updateMetric("failed_queries", 1)
		return "", fmt.Errorf("不支持的SQL语句类型")
	}
}

// executeSelect 执行SELECT语句
func (s *Server) executeSelect(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 从数据库中获取所有匹配前缀的键
	keys, err := s.db.PrefixScan(tablePrefix)
	if err != nil {
		return "", fmt.Errorf("获取表数据失败: %w", err)
	}

	// 将键按记录ID分组
	recordMap := make(map[string]map[string]string)
	for _, key := range keys {
		parts := strings.SplitN(key[len(tablePrefix):], ":", 2)
		if len(parts) != 2 {
			continue
		}

		recordID := parts[0]
		columnName := parts[1]

		// 获取值
		value, err := tx.Get(key)
		if err != nil {
			continue
		}

		// 将记录添加到映射
		if _, exists := recordMap[recordID]; !exists {
			recordMap[recordID] = make(map[string]string)
		}
		recordMap[recordID][columnName] = string(value)
	}

	// 如果没有要查询的特定列，使用表中第一条记录的所有列名
	if len(stmt.Columns) == 0 || (len(stmt.Columns) == 1 && stmt.Columns[0] == "*") {
		for _, record := range recordMap {
			stmt.Columns = make([]string, 0, len(record))
			for col := range record {
				stmt.Columns = append(stmt.Columns, col)
			}
			break
		}
	}

	// 筛选符合WHERE条件的记录
	filteredRecords := make([]map[string]string, 0)
	for recordID, record := range recordMap {
		// 如果有WHERE条件，检查记录是否符合
		if stmt.Where != nil {
			// 判断是否满足条件
			match, err := s.evaluateRecordCondition(stmt.Where, record)
			if err != nil {
				continue
			}
			if !match {
				continue
			}
		}

		// 添加记录ID
		record["_id"] = recordID
		filteredRecords = append(filteredRecords, record)
	}

	// 处理排序（如果有）
	if len(stmt.OrderBy) > 0 {
		sort.Slice(filteredRecords, func(i, j int) bool {
			for _, order := range stmt.OrderBy {
				colName := order.Column
				valI, existsI := filteredRecords[i][colName]
				valJ, existsJ := filteredRecords[j][colName]

				// 如果其中一个值不存在，认为它小于存在的值
				if !existsI && existsJ {
					return !order.Desc // 升序时不存在的值在前，降序时存在的值在前
				}
				if existsI && !existsJ {
					return order.Desc // 升序时存在的值在前，降序时不存在的值在前
				}
				if !existsI && !existsJ {
					continue // 都不存在，比较下一个列
				}

				// 尝试按数字比较
				numI, errI := strconv.ParseFloat(valI, 64)
				numJ, errJ := strconv.ParseFloat(valJ, 64)
				if errI == nil && errJ == nil {
					if numI != numJ {
						return (numI < numJ) != order.Desc // 升序或降序
					}
				} else {
					// 按字符串比较
					if valI != valJ {
						if order.Desc {
							return valI > valJ // 降序
						}
						return valI < valJ // 升序
					}
				}
			}
			return false // 所有排序键都相等
		})
	}

	// 处理分组（如果有）
	if len(stmt.GroupBy) > 0 {
		// 按分组键分组
		groupMap := make(map[string][]map[string]string)
		for _, record := range filteredRecords {
			groupKey := ""
			for _, col := range stmt.GroupBy {
				groupKey += record[col] + "|"
			}
			groupMap[groupKey] = append(groupMap[groupKey], record)
		}

		// 重构记录列表，每个分组保留一条记录
		filteredRecords = make([]map[string]string, 0, len(groupMap))
		for _, group := range groupMap {
			if len(group) > 0 {
				groupRecord := make(map[string]string)
				// 复制分组键
				for _, col := range stmt.GroupBy {
					groupRecord[col] = group[0][col]
				}
				// 处理聚合函数
				for _, agg := range stmt.AggFuncs {
					switch agg.Name {
					case "COUNT":
						groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = strconv.Itoa(len(group))
					case "SUM":
						var sum float64
						for _, record := range group {
							if val, ok := record[agg.Column]; ok {
								if num, err := strconv.ParseFloat(val, 64); err == nil {
									sum += num
								}
							}
						}
						groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = strconv.FormatFloat(sum, 'f', 2, 64)
					case "AVG":
						var sum float64
						var count int
						for _, record := range group {
							if val, ok := record[agg.Column]; ok {
								if num, err := strconv.ParseFloat(val, 64); err == nil {
									sum += num
									count++
								}
							}
						}
						var avg float64
						if count > 0 {
							avg = sum / float64(count)
						}
						groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = strconv.FormatFloat(avg, 'f', 2, 64)
					case "MAX":
						var max float64
						var found bool
						for _, record := range group {
							if val, ok := record[agg.Column]; ok {
								if num, err := strconv.ParseFloat(val, 64); err == nil {
									if !found || num > max {
										max = num
										found = true
									}
								}
							}
						}
						if found {
							groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = strconv.FormatFloat(max, 'f', 2, 64)
						} else {
							groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = "0"
						}
					case "MIN":
						var min float64
						var found bool
						for _, record := range group {
							if val, ok := record[agg.Column]; ok {
								if num, err := strconv.ParseFloat(val, 64); err == nil {
									if !found || num < min {
										min = num
										found = true
									}
								}
							}
						}
						if found {
							groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = strconv.FormatFloat(min, 'f', 2, 64)
						} else {
							groupRecord[fmt.Sprintf("%s(%s)", agg.Name, agg.Column)] = "0"
						}
					}
				}
				filteredRecords = append(filteredRecords, groupRecord)
			}
		}
	}

	// 处理LIMIT和OFFSET
	if stmt.Limit >= 0 {
		start := stmt.Offset
		end := start + stmt.Limit
		if start >= len(filteredRecords) {
			start = len(filteredRecords)
		}
		if end > len(filteredRecords) {
			end = len(filteredRecords)
		}
		filteredRecords = filteredRecords[start:end]
	}

	// 构建结果字符串
	var result strings.Builder

	// 添加表头
	for i, col := range stmt.Columns {
		if i > 0 {
			result.WriteString("\t")
		}
		result.WriteString(col)
	}
	result.WriteString("\n")

	// 添加记录
	for _, record := range filteredRecords {
		for i, col := range stmt.Columns {
			if i > 0 {
				result.WriteString("\t")
			}
			result.WriteString(record[col])
		}
		result.WriteString("\n")
	}

	// 添加记录计数
	result.WriteString(fmt.Sprintf("\n共 %d 条记录", len(filteredRecords)))

	return result.String(), nil
}

// evaluateRecordCondition 评估记录是否满足条件
func (s *Server) evaluateRecordCondition(cond *parser.Condition, record map[string]string) (bool, error) {
	// 获取字段值
	value, exists := record[cond.Column]

	// 根据操作符类型比较
	switch cond.Operator {
	case "=":
		if !exists {
			return false, nil
		}
		return value == cond.Value, nil
	case "!=", "<>":
		if !exists {
			return true, nil // 不存在视为不等
		}
		return value != cond.Value, nil
	case ">":
		if !exists {
			return false, nil
		}
		// 尝试数值比较
		numVal, err1 := strconv.ParseFloat(value, 64)
		numCond, err2 := strconv.ParseFloat(cond.Value, 64)
		if err1 == nil && err2 == nil {
			return numVal > numCond, nil
		}
		// 字符串比较
		return value > cond.Value, nil
	case ">=":
		if !exists {
			return false, nil
		}
		// 尝试数值比较
		numVal, err1 := strconv.ParseFloat(value, 64)
		numCond, err2 := strconv.ParseFloat(cond.Value, 64)
		if err1 == nil && err2 == nil {
			return numVal >= numCond, nil
		}
		// 字符串比较
		return value >= cond.Value, nil
	case "<":
		if !exists {
			return false, nil
		}
		// 尝试数值比较
		numVal, err1 := strconv.ParseFloat(value, 64)
		numCond, err2 := strconv.ParseFloat(cond.Value, 64)
		if err1 == nil && err2 == nil {
			return numVal < numCond, nil
		}
		// 字符串比较
		return value < cond.Value, nil
	case "<=":
		if !exists {
			return false, nil
		}
		// 尝试数值比较
		numVal, err1 := strconv.ParseFloat(value, 64)
		numCond, err2 := strconv.ParseFloat(cond.Value, 64)
		if err1 == nil && err2 == nil {
			return numVal <= numCond, nil
		}
		// 字符串比较
		return value <= cond.Value, nil
	case "LIKE":
		if !exists {
			return false, nil
		}
		// 简化的LIKE操作，将%转换为.*
		pattern := strings.ReplaceAll(cond.Value, "%", ".*")
		matched, err := regexp.MatchString("^"+pattern+"$", value)
		if err != nil {
			return false, err
		}
		return matched, nil
	case "IN":
		if !exists {
			return false, nil
		}
		// 检查值是否在列表中
		for _, inValue := range cond.Values {
			if value == inValue {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("不支持的操作符: %s", cond.Operator)
	}
}

// executeInsert 执行INSERT语句
func (s *Server) executeInsert(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if len(stmt.Values) == 0 {
		return "", fmt.Errorf("没有要插入的值")
	}

	// 获取自增ID（如果需要）
	recordID := ""
	hasIDColumn := false
	idColumnIndex := -1

	// 检查是否有ID列
	for i, col := range stmt.Columns {
		if col == "id" || col == "ID" {
			hasIDColumn = true
			idColumnIndex = i
			break
		}
	}

	// 处理每一组值
	affected := 0
	for _, values := range stmt.Values {
		// 如果指定了ID列且提供了值，使用它
		if hasIDColumn && idColumnIndex < len(values) && values[idColumnIndex] != "" {
			recordID = values[idColumnIndex]
		} else {
			// 否则使用自增ID
			nextID := s.db.GetNextID(stmt.Table)
			recordID = strconv.Itoa(nextID)

			// 如果有ID列但没提供值，设置自增值
			if hasIDColumn && idColumnIndex < len(values) {
				values[idColumnIndex] = recordID
			}
		}

		// 插入每一列
		for i, col := range stmt.Columns {
			if i < len(values) {
				key := fmt.Sprintf("%s:%s:%s", stmt.Table, recordID, col)
				if err := tx.Put(key, []byte(values[i])); err != nil {
					return "", fmt.Errorf("插入数据失败: %w", err)
				}
			}
		}

		affected++
	}

	return fmt.Sprintf("成功: 插入了 %d 条记录", affected), nil
}

// executeUpdate 执行UPDATE语句
func (s *Server) executeUpdate(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 从数据库中获取所有匹配前缀的键
	keys, err := s.db.PrefixScan(tablePrefix)
	if err != nil {
		return "", fmt.Errorf("获取表数据失败: %w", err)
	}

	// 将键按记录ID分组
	recordMap := make(map[string]map[string]string)
	for _, key := range keys {
		parts := strings.SplitN(key[len(tablePrefix):], ":", 2)
		if len(parts) != 2 {
			continue
		}

		recordID := parts[0]
		columnName := parts[1]

		// 获取值
		value, err := tx.Get(key)
		if err != nil {
			continue
		}

		// 将记录添加到映射
		if _, exists := recordMap[recordID]; !exists {
			recordMap[recordID] = make(map[string]string)
		}
		recordMap[recordID][columnName] = string(value)
	}

	// 筛选符合WHERE条件的记录并更新
	updatedCount := 0
	for recordID, record := range recordMap {
		// 如果有WHERE条件，检查记录是否符合
		if stmt.Where != nil {
			// 判断是否满足条件
			match, err := s.evaluateRecordCondition(stmt.Where, record)
			if err != nil {
				continue
			}
			if !match {
				continue
			}
		}

		// 更新符合条件的记录
		for i, col := range stmt.Columns {
			if i < len(stmt.Values[0]) {
				key := fmt.Sprintf("%s:%s:%s", stmt.Table, recordID, col)
				if err := tx.Put(key, []byte(stmt.Values[0][i])); err != nil {
					return "", fmt.Errorf("更新数据失败: %w", err)
				}
			}
		}

		updatedCount++
	}

	return fmt.Sprintf("成功: 更新了 %d 条记录", updatedCount), nil
}

// executeDelete 执行DELETE语句
func (s *Server) executeDelete(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 从数据库中获取所有匹配前缀的键
	keys, err := s.db.PrefixScan(tablePrefix)
	if err != nil {
		return "", fmt.Errorf("获取表数据失败: %w", err)
	}

	// 将键按记录ID分组
	recordMap := make(map[string]map[string]string)
	recordKeys := make(map[string][]string)

	for _, key := range keys {
		parts := strings.SplitN(key[len(tablePrefix):], ":", 2)
		if len(parts) != 2 {
			continue
		}

		recordID := parts[0]
		columnName := parts[1]

		// 获取值
		value, err := tx.Get(key)
		if err != nil {
			continue
		}

		// 将记录添加到映射
		if _, exists := recordMap[recordID]; !exists {
			recordMap[recordID] = make(map[string]string)
			recordKeys[recordID] = make([]string, 0)
		}
		recordMap[recordID][columnName] = string(value)
		recordKeys[recordID] = append(recordKeys[recordID], key)
	}

	// 筛选符合WHERE条件的记录并删除
	deletedCount := 0
	for recordID, record := range recordMap {
		// 如果有WHERE条件，检查记录是否符合
		if stmt.Where != nil {
			// 判断是否满足条件
			match, err := s.evaluateRecordCondition(stmt.Where, record)
			if err != nil {
				continue
			}
			if !match {
				continue
			}
		}

		// 删除符合条件的记录的所有键
		for _, key := range recordKeys[recordID] {
			if err := tx.Delete(key); err != nil {
				return "", fmt.Errorf("删除数据失败: %w", err)
			}
		}

		deletedCount++
	}

	return fmt.Sprintf("成功: 删除了 %d 条记录", deletedCount), nil
}

// executeCreateTable 执行CREATE TABLE语句
func (s *Server) executeCreateTable(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 在实际实现中，我们只需要记录表结构
	// 创建一个元数据键来存储表结构
	tableMetaKey := fmt.Sprintf("table_meta:%s", stmt.Table)

	// 将列名列表序列化为字符串
	columnsStr := strings.Join(stmt.Columns, ",")

	// 存储表元数据
	if err := tx.Put(tableMetaKey, []byte(columnsStr)); err != nil {
		return "", fmt.Errorf("创建表失败: %w", err)
	}

	return fmt.Sprintf("成功: 表 %s 已创建", stmt.Table), nil
}

// executeCreateIndex 执行CREATE INDEX语句
func (s *Server) executeCreateIndex(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 获取索引类型
	indexType := storage.BPlusTreeIndex
	if stmt.IndexType > 0 {
		indexType = stmt.IndexType
	}

	// 创建索引
	if err := s.db.CreateIndex(stmt.Table, stmt.IndexName, stmt.Columns, indexType, stmt.Unique); err != nil {
		return "", fmt.Errorf("创建索引失败: %w", err)
	}

	if stmt.Unique {
		return fmt.Sprintf("成功: 唯一索引 %s 已创建", stmt.IndexName), nil
	}
	return fmt.Sprintf("成功: 索引 %s 已创建", stmt.IndexName), nil
}

// executeDropIndex 执行DROP INDEX语句
func (s *Server) executeDropIndex(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 删除索引
	if err := s.db.DropIndex(stmt.Table, stmt.IndexName); err != nil {
		return "", fmt.Errorf("删除索引失败: %w", err)
	}

	return fmt.Sprintf("成功: 索引 %s 已删除", stmt.IndexName), nil
}

// updateMetric 更新性能指标
func (s *Server) updateMetric(name string, value int64) {
	s.metricsMutex.Lock()
	defer s.metricsMutex.Unlock()

	s.metrics[name] += value
}

// GetMetrics 获取所有性能指标
func (s *Server) GetMetrics() map[string]int64 {
	s.metricsMutex.Lock()
	defer s.metricsMutex.Unlock()

	// 创建指标的副本
	result := make(map[string]int64, len(s.metrics))
	for k, v := range s.metrics {
		result[k] = v
	}

	// 添加当前活跃查询数
	result["active_queries"] = int64(atomic.LoadInt32(&s.activeQueries))

	return result
}
