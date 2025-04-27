package server

import (
	"bytes"
	"context"
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

// Server 表示数据库服务器
type Server struct {
	host           string
	port           int
	db             *storage.DB
	txManager      *transaction.Manager
	parser         *parser.Parser
	listener       net.Listener
	mutex          sync.RWMutex
	running        bool
	conns          map[string]net.Conn // 跟踪活跃连接
	connMutex      sync.Mutex          // 连接映射的互斥锁
	charset        string              // 字符集编码
	connTimeout    time.Duration       // 连接超时时间
	queryTimeout   time.Duration       // 查询超时时间
	maxConnections int                 // 最大连接数
	activeQueries  int32               // 当前活跃查询数
	metrics        map[string]int64    // 性能指标统计
	metricsMutex   sync.Mutex          // 指标互斥锁
	wg             sync.WaitGroup      // 等待组，用于等待所有连接处理完毕
}

// NewServer 创建一个新的数据库服务器
func NewServer(host string, port int, db *storage.DB) *Server {
	txManager := transaction.NewManager(db, 30*time.Second) // 默认30秒超时
	return &Server{
		host:           host,
		port:           port,
		db:             db,
		txManager:      txManager,
		parser:         parser.NewParser(),
		running:        false,
		conns:          make(map[string]net.Conn),
		charset:        "utf8mb4",        // 默认使用utf8mb4字符集
		connTimeout:    5 * time.Minute,  // 默认连接超时时间
		queryTimeout:   30 * time.Second, // 默认查询超时时间
		maxConnections: 100,              // 默认最大连接数
		activeQueries:  0,
		metrics:        make(map[string]int64),
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
	// 简化版MySQL握手包
	handshake := []byte{
		10,                              // 协议版本
		'5', '.', '7', '.', '2', '5', 0, // 服务器版本
		1, 0, 0, 0, // 连接ID (简化为1)
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', // 认证插件数据前8字节
		0,    // 填充
		8, 0, // 服务器能力标志低位
		33,   // 字符集 (utf8_general_ci)
		2, 0, // 服务器状态
		8, 0, // 服务器能力标志高位
		21,                           // 认证数据长度
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 保留10字节0
		'I', 'J', 'K', 'L', 'M', // 认证插件数据剩余部分
		'm', 'y', 's', 'q', 'l', '_', 'n', 'a', 't', 'i', 'v', 'e', '_', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd', 0, // 认证插件名
	}

	header := []byte{
		byte(len(handshake)),
		0,
		0,
		0,
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(handshake); err != nil {
		return err
	}

	return nil
}

// 处理客户端认证
func (s *Server) handleClientAuth(conn net.Conn) (string, error) {
	// 读取认证包
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}

	packetLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	authData := make([]byte, packetLen)
	if _, err := io.ReadFull(conn, authData); err != nil {
		return "", err
	}

	// 提取用户名
	// MySQL认证包格式: [capability_flags(4)][max_packet_size(4)][charset(1)][reserved(23)][username][NUL][auth_response]
	// 简化处理，仅提取用户名
	usernameEnd := bytes.IndexByte(authData[32:], 0)
	if usernameEnd == -1 {
		return "", fmt.Errorf("无效的认证包")
	}

	username := string(authData[32 : 32+usernameEnd])

	// 在实际应用中，这里应该验证用户名和密码
	// 为简化示例，我们接受任何用户名

	return username, nil
}

// 发送OK包
func (s *Server) sendOKPacket(conn net.Conn) error {
	// OK包: [00][affected_rows][last_insert_id][status_flags][warnings][info]
	okPacket := []byte{
		0,    // OK包标识
		0,    // 受影响的行数
		0,    // 最后插入的ID
		2, 0, // 状态标志
		0, 0, // 警告数
	}

	header := []byte{
		byte(len(okPacket)),
		0,
		0,
		1, // 包序号
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(okPacket); err != nil {
		return err
	}

	return nil
}

// 发送错误包
func (s *Server) sendErrorPacket(conn net.Conn, errCode int, errMsg string) error {
	// 错误包: [FF][error_code][sql_state_marker][sql_state][error_message]
	errPacket := make([]byte, 0, len(errMsg)+13)
	errPacket = append(errPacket, 0xFF)                            // 错误包标识
	errPacket = append(errPacket, byte(errCode), byte(errCode>>8)) // 错误码
	errPacket = append(errPacket, '#')                             // SQL状态标记
	errPacket = append(errPacket, '0', '8', '0', '0', '0')         // SQL状态
	errPacket = append(errPacket, errMsg...)                       // 错误消息

	header := []byte{
		byte(len(errPacket)),
		byte(len(errPacket) >> 8),
		byte(len(errPacket) >> 16),
		1, // 包序号
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(errPacket); err != nil {
		return err
	}

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
	// 简化的结果集处理
	// 1. 发送列数
	columnCount := []byte{1} // 假设只有一列

	headerCol := []byte{
		byte(len(columnCount)),
		0,
		0,
		1, // 包序号
	}

	if _, err := conn.Write(headerCol); err != nil {
		return err
	}
	if _, err := conn.Write(columnCount); err != nil {
		return err
	}

	// 2. 发送列定义
	colDef := []byte{
		3, 'd', 'e', 'f', // catalog
		0,                               // schema
		6, 'r', 'e', 's', 'u', 'l', 't', // table
		6, 'r', 'e', 's', 'u', 'l', 't', // org_table
		6, 'R', 'e', 's', 'u', 'l', 't', // name
		6, 'R', 'e', 's', 'u', 'l', 't', // org_name
		12,      // 列定义长度
		0x0C, 0, // 字符集
		255, 0, 0, 0, // 列长度
		0xFD, // 列类型 (VAR_STRING)
		0, 0, // 标志
		0,    // 小数点位数
		0, 0, // 保留
	}

	headerDef := []byte{
		byte(len(colDef)),
		byte(len(colDef) >> 8),
		0,
		2, // 包序号
	}

	if _, err := conn.Write(headerDef); err != nil {
		return err
	}
	if _, err := conn.Write(colDef); err != nil {
		return err
	}

	// 3. 发送EOF
	eof := []byte{
		0xFE, // EOF标识
		0, 0, // 警告数
		2, 0, // 状态标志
	}

	headerEof := []byte{
		byte(len(eof)),
		0,
		0,
		3, // 包序号
	}

	if _, err := conn.Write(headerEof); err != nil {
		return err
	}
	if _, err := conn.Write(eof); err != nil {
		return err
	}

	// 4. 发送数据行
	row := make([]byte, 0, len(result)+1)
	row = append(row, byte(len(result))) // 列长度
	row = append(row, result...)         // 列值

	headerRow := []byte{
		byte(len(row)),
		byte(len(row) >> 8),
		0,
		4, // 包序号
	}

	if _, err := conn.Write(headerRow); err != nil {
		return err
	}
	if _, err := conn.Write(row); err != nil {
		return err
	}

	// 5. 发送EOF
	headerEofEnd := []byte{
		byte(len(eof)),
		0,
		0,
		5, // 包序号
	}

	if _, err := conn.Write(headerEofEnd); err != nil {
		return err
	}
	if _, err := conn.Write(eof); err != nil {
		return err
	}

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
