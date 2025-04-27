package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

// SimpleServer 表示简化版数据库服务器，不依赖其他包
type SimpleServer struct {
	host      string
	port      int
	dataDir   string
	listener  net.Listener
	running   bool
	mutex     sync.Mutex
	wg        sync.WaitGroup
	conns     map[string]net.Conn // 跟踪活跃连接
	connMutex sync.Mutex          // 连接映射的互斥锁
}

// NewSimpleServer 创建新的简化版服务器实例
func NewSimpleServer(host string, port int, dataDir string) *SimpleServer {
	return &SimpleServer{
		host:    host,
		port:    port,
		dataDir: dataDir,
		running: false,
		conns:   make(map[string]net.Conn),
	}
}

// Start 启动服务器
func (s *SimpleServer) Start() error {
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

// acceptConnections 接受并处理客户端连接
func (s *SimpleServer) acceptConnections() {
	defer s.wg.Done()

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running {
				log.Printf("接受连接时出错: %v", err)
			}
			break
		}

		clientAddr := conn.RemoteAddr().String()

		s.connMutex.Lock()
		s.conns[clientAddr] = conn
		s.connMutex.Unlock()

		s.wg.Add(1)
		go func(id string, c net.Conn) {
			s.handleConnection(c)

			// 连接处理完毕后移除
			s.connMutex.Lock()
			delete(s.conns, id)
			s.connMutex.Unlock()
		}(clientAddr, conn)
	}
}

// handleConnection 处理单个客户端连接
func (s *SimpleServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	log.Printf("新连接来自 %s", clientAddr)

	// 发送欢迎消息
	welcomeMsg := "欢迎使用 Cyber-DB (简化版)\r\n输入SQL命令或输入 'exit' 退出\r\n> "
	_, err := conn.Write([]byte(welcomeMsg))
	if err != nil {
		log.Printf("向客户端 %s 发送数据时出错: %v", clientAddr, err)
		return
	}

	// 简单的命令处理循环
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		cmd := scanner.Text()
		cmd = strings.TrimSpace(cmd)

		if cmd == "" {
			conn.Write([]byte("> "))
			continue
		}

		if strings.ToLower(cmd) == "exit" {
			conn.Write([]byte("再见!\r\n"))
			break
		}

		// 简单的命令回显
		response := fmt.Sprintf("执行: %s\r\n> ", cmd)
		_, err := conn.Write([]byte(response))
		if err != nil {
			log.Printf("向客户端 %s 发送数据时出错: %v", clientAddr, err)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("从客户端 %s 读取数据时出错: %v", clientAddr, err)
	}

	log.Printf("连接关闭: %s", clientAddr)
}

// Stop 停止服务器
func (s *SimpleServer) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return nil
	}

	s.running = false

	// 关闭所有活跃连接
	s.connMutex.Lock()
	for id, conn := range s.conns {
		conn.Close()
		delete(s.conns, id)
	}
	s.connMutex.Unlock()

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("关闭监听器时出错: %v", err)
		}
	}

	s.wg.Wait()
	log.Println("服务器已停止")
	return nil
}
