package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
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
		log.Printf("Connection limit reached (%d), rejecting connection from %s\n", s.maxConnections, conn.RemoteAddr())
		fmt.Fprintf(conn, "Error: Server connection limit reached. Please try again later.\n")
		return
	}

	log.Printf("New connection from %s (active connections: %d/%d)\n", conn.RemoteAddr(), connCount+1, s.maxConnections)

	// 设置连接超时
	conn.SetDeadline(time.Now().Add(s.connTimeout))

	// 发送欢迎消息
	fmt.Fprintf(conn, "Welcome to Cyber-DB Database (Charset: %s)\n", s.charset)
	fmt.Fprintf(conn, "Enter SQL statements or 'exit' to quit\n")

	// 创建一个事务
	tx, err := s.txManager.Begin()
	if err != nil {
		log.Printf("Error creating transaction for connection %s: %v\n", conn.RemoteAddr(), err)
		fmt.Fprintf(conn, "Error: %v\n", err)
		return
	}

	// 创建一个上下文，用于处理超时
	ctx, cancel := context.WithTimeout(context.Background(), s.connTimeout)
	defer cancel()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		// 重置连接超时
		conn.SetDeadline(time.Now().Add(s.connTimeout))

		// 重置查询超时上下文
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), s.queryTimeout)

		cmd := scanner.Text()
		cmd = strings.TrimSpace(cmd)

		if cmd == "" {
			continue
		}

		if strings.ToLower(cmd) == "exit" {
			// 提交事务并关闭连接
			if err := tx.Commit(); err != nil {
				fmt.Fprintf(conn, "Error committing transaction: %v\n", err)
			}
			fmt.Fprintf(conn, "Goodbye!\n")
			break
		}

		// 解析SQL语句
		stmt, err := s.parser.Parse(cmd)
		if err != nil {
			fmt.Fprintf(conn, "Error: %v\n", err)
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
				fmt.Fprintf(conn, "Error creating new transaction: %v\n", err)
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
			// 返回结果
			fmt.Fprintf(conn, "%s\n", result)
		case err := <-errCh:
			fmt.Fprintf(conn, "Error: %v\n", err)
			// 检查是否需要创建新事务
			if tx.Status() != transaction.TxStatusActive {
				tx, err = s.txManager.Begin()
				if err != nil {
					fmt.Fprintf(conn, "Error creating new transaction: %v\n", err)
				}
			}
		case <-ctx.Done():
			fmt.Fprintf(conn, "Error: query execution timeout\n")
			// 超时后回滚事务并创建新事务
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				log.Printf("Error rolling back transaction after timeout: %v\n", rollbackErr)
			}
			tx, err = s.txManager.Begin()
			if err != nil {
				fmt.Fprintf(conn, "Error creating new transaction: %v\n", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v\n", err)
	}

	log.Printf("连接关闭: %s", clientAddr)
}

// executeStatement 执行SQL语句
func (s *Server) executeStatement(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	// 记录开始时间，用于计算查询耗时
	startTime := time.Now()

	// 更新活跃查询计数
	atomic.AddInt32(&s.activeQueries, 1)
	defer atomic.AddInt32(&s.activeQueries, -1)

	// 检查事务状态和超时
	txStatus := tx.Status()
	if txStatus != transaction.TxStatusActive {
		return "", fmt.Errorf("transaction not active or timeout")
	}

	// 执行语句并处理错误
	var result string
	var err error

	// 记录查询类型
	queryType := "unknown"
	switch stmt.Type {
	case parser.InsertStmt:
		queryType = "insert"
		result, err = s.executeInsert(stmt, tx)
	case parser.SelectStmt:
		queryType = "select"
		result, err = s.executeSelect(stmt, tx)
	case parser.UpdateStmt:
		queryType = "update"
		result, err = s.executeUpdate(stmt, tx)
	case parser.DeleteStmt:
		queryType = "delete"
		result, err = s.executeDelete(stmt, tx)
	case parser.CreateTableStmt:
		queryType = "create_table"
		result, err = s.executeCreateTable(stmt, tx)
	case parser.CreateIndexStmt:
		queryType = "create_index"
		result, err = s.executeCreateIndex(stmt, tx)
	case parser.DropIndexStmt:
		queryType = "drop_index"
		result, err = s.executeDropIndex(stmt, tx)
	default:
		err = fmt.Errorf("unsupported statement type: %v", stmt.Type)
	}

	// 再次检查事务状态，确保事务在执行过程中没有超时
	txStatus = tx.Status()
	if txStatus != transaction.TxStatusActive {
		// 更新事务超时计数
		s.updateMetric("tx_timeout_count", 1)
		return "", fmt.Errorf("transaction became inactive during execution")
	}

	// 处理执行错误
	if err != nil {
		// 更新错误计数
		s.updateMetric("query_error_count", 1)
		s.updateMetric(fmt.Sprintf("%s_error_count", queryType), 1)

		// 回滚事务并返回错误
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			// 如果回滚也失败，记录错误并返回组合错误信息
			log.Printf("Error rolling back transaction: %v\n", rollbackErr)
			s.updateMetric("rollback_error_count", 1)
			return "", fmt.Errorf("execution failed: %v; rollback failed: %v", err, rollbackErr)
		}
		return "", fmt.Errorf("execution failed: %v", err)
	}

	// 计算查询耗时
	elapsedTime := time.Since(startTime)
	elapsedMs := elapsedTime.Milliseconds()

	// 更新性能指标
	s.updateMetric("query_count", 1)
	s.updateMetric(fmt.Sprintf("%s_count", queryType), 1)
	s.updateMetric("query_time_total_ms", elapsedMs)

	// 添加查询耗时到结果中
	result = fmt.Sprintf("%s\n查询耗时: %v", result, elapsedTime.Round(time.Millisecond))

	return result, nil
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

// executeSelect 执行SELECT语句
func (s *Server) executeSelect(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" {
		return "", fmt.Errorf("table name is required")
	}

	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 收集结果
	results := []string{}

	// 处理聚合函数
	if len(stmt.AggFuncs) > 0 {
		for _, aggFunc := range stmt.AggFuncs {
			// 获取列值
			key := fmt.Sprintf("%s%s", tablePrefix, aggFunc.Column)
			value, err := tx.Get(key)
			if err != nil {
				// 如果找不到值，跳过
				continue
			}

			// 将值转换为数字（如果可能）
			strValue := string(value)
			numValue, err := strconv.ParseFloat(strValue, 64)
			if err != nil {
				// 如果不是数字，对于SUM、AVG和MIN/MAX，分别返回0或原始值
				switch aggFunc.Name {
				case "SUM", "AVG":
					results = append(results, fmt.Sprintf("%s(%s) = 0", aggFunc.Name, aggFunc.Column))
				case "MAX", "MIN":
					results = append(results, fmt.Sprintf("%s(%s) = %s", aggFunc.Name, aggFunc.Column, strValue))
				}
				continue
			}

			// 根据聚合函数类型处理
			switch aggFunc.Name {
			case "SUM":
				results = append(results, fmt.Sprintf("SUM(%s) = %g", aggFunc.Column, numValue))
			case "MAX":
				results = append(results, fmt.Sprintf("MAX(%s) = %g", aggFunc.Column, numValue))
			case "MIN":
				results = append(results, fmt.Sprintf("MIN(%s) = %g", aggFunc.Column, numValue))
			case "AVG":
				results = append(results, fmt.Sprintf("AVG(%s) = %g", aggFunc.Column, numValue))
			}
		}
	}

	// 如果有WHERE条件，执行过滤查询
	if stmt.Where != nil {
		// 处理WHERE条件
		result, err := s.evaluateCondition(stmt.Where, tablePrefix, tx)
		if err != nil {
			return "", err
		}

		// 如果条件评估为真，添加结果
		if result != "" {
			results = append(results, result)
		}
	} else if len(stmt.AggFuncs) == 0 { // 只有在没有聚合函数时才执行全表扫描
		// 尝试使用索引进行查询
		indexManager := s.db.GetIndexManager()
		if indexManager != nil {
			// 查找可用的索引
			for _, col := range stmt.Columns {
				// 这里简化处理，实际应该根据查询条件选择最优索引
				index := indexManager.GetTableIndex(stmt.Table, col)
				if index != nil {
					// 使用索引查询
					results = append(results, fmt.Sprintf("Using index: %s", index.Config.Name))
					break
				}
			}
		}

		// 如果没有找到索引，执行全表扫描
		if len(results) == 0 {
			results = append(results, "Full table scan")
		}
	}

	// 处理GROUP BY子句
	if len(stmt.GroupBy) > 0 {
		// 简化实现，仅显示分组信息
		groupByInfo := fmt.Sprintf("Grouped by: %s", strings.Join(stmt.GroupBy, ", "))
		results = append(results, groupByInfo)
	}

	// 处理ORDER BY子句
	if len(stmt.OrderBy) > 0 {
		orderByInfo := "Ordered by: "
		orderItems := []string{}

		for _, item := range stmt.OrderBy {
			orderDir := "ASC"
			if item.Desc {
				orderDir = "DESC"
			}
			orderItems = append(orderItems, fmt.Sprintf("%s %s", item.Column, orderDir))
		}

		orderByInfo += strings.Join(orderItems, ", ")
		results = append(results, orderByInfo)
	}

	// 处理LIMIT和OFFSET子句
	paginationInfo := ""
	if stmt.Limit >= 0 {
		paginationInfo = fmt.Sprintf("LIMIT %d", stmt.Limit)
		if stmt.Offset > 0 {
			paginationInfo += fmt.Sprintf(" OFFSET %d", stmt.Offset)
		}
		results = append(results, paginationInfo)
	} else if stmt.Offset > 0 {
		paginationInfo = fmt.Sprintf("OFFSET %d", stmt.Offset)
		results = append(results, paginationInfo)
	}

	return fmt.Sprintf("SELECT result:\n%s", strings.Join(results, "\n")), nil
}

// evaluateCondition 评估WHERE条件
func (s *Server) evaluateCondition(cond *parser.Condition, tablePrefix string, tx *transaction.Transaction) (string, error) {
	// 处理IN查询
	if cond.Operator == "IN" && len(cond.Values) > 0 {
		// 获取列值
		key := fmt.Sprintf("%s%s", tablePrefix, cond.Column)
		value, err := tx.Get(key)
		if err != nil {
			return "", nil // 键不存在，条件不满足
		}

		// 将值转换为字符串进行比较
		strValue := string(value)

		// 检查值是否在IN列表中
		for _, v := range cond.Values {
			if strValue == v {
				// 匹配成功
				if cond.Next != nil {
					// 处理下一个条件
					nextResult, err := s.evaluateCondition(cond.Next, tablePrefix, tx)
					if err != nil {
						return "", err
					}

					// 根据逻辑运算符组合结果
					switch cond.NextLogic {
					case "AND":
						if nextResult == "" {
							return "", nil // AND条件不满足
						}
						return fmt.Sprintf("%s IN (%s), %s", cond.Column, strings.Join(cond.Values, ", "), nextResult), nil
					case "OR":
						if nextResult != "" {
							return fmt.Sprintf("%s IN (%s), %s", cond.Column, strings.Join(cond.Values, ", "), nextResult), nil
						}
						return fmt.Sprintf("%s IN (%s)", cond.Column, strings.Join(cond.Values, ", ")), nil
					default:
						return "", fmt.Errorf("unsupported logic operator: %s", cond.NextLogic)
					}
				}

				// 没有下一个条件，直接返回结果
				return fmt.Sprintf("%s IN (%s)", cond.Column, strings.Join(cond.Values, ", ")), nil
			}
		}

		// 不匹配，检查是否有OR条件
		if cond.Next != nil && cond.NextLogic == "OR" {
			return s.evaluateCondition(cond.Next, tablePrefix, tx)
		}

		return "", nil // 条件不满足
	}

	// 获取列值
	key := fmt.Sprintf("%s%s", tablePrefix, cond.Column)
	value, err := tx.Get(key)
	if err != nil {
		return "", nil // 键不存在，条件不满足
	}

	// 将值转换为字符串进行比较
	strValue := string(value)

	// 根据运算符进行比较
	matched := false
	switch cond.Operator {
	case "=":
		matched = strValue == cond.Value
	case "!=":
		matched = strValue != cond.Value
	case ">":
		matched = strValue > cond.Value
	case "<":
		matched = strValue < cond.Value
	case ">=":
		matched = strValue >= cond.Value
	case "<=":
		matched = strValue <= cond.Value
	case "LIKE":
		// 将SQL的LIKE模式转换为正则表达式
		pattern := strings.ReplaceAll(cond.Value, "%", ".*")
		pattern = strings.ReplaceAll(pattern, "_", ".")
		pattern = "^" + pattern + "$"
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return "", fmt.Errorf("invalid LIKE pattern: %s", err)
		}
		matched = regex.MatchString(strValue)
	default:
		return "", fmt.Errorf("unsupported operator: %s", cond.Operator)
	}

	// 如果条件匹配
	if matched {
		// 如果有下一个条件
		if cond.Next != nil {
			nextResult, err := s.evaluateCondition(cond.Next, tablePrefix, tx)
			if err != nil {
				return "", err
			}

			// 根据逻辑运算符组合结果
			switch cond.NextLogic {
			case "AND":
				if nextResult == "" {
					return "", nil // AND条件不满足
				}
				return fmt.Sprintf("%s%s%s, %s", cond.Column, cond.Operator, strValue, nextResult), nil
			case "OR":
				if nextResult != "" {
					return fmt.Sprintf("%s%s%s, %s", cond.Column, cond.Operator, strValue, nextResult), nil
				}
				return fmt.Sprintf("%s%s%s", cond.Column, cond.Operator, strValue), nil
			default:
				return "", fmt.Errorf("unsupported logic operator: %s", cond.NextLogic)
			}
		}

		// 没有下一个条件，直接返回结果
		return fmt.Sprintf("%s%s%s", cond.Column, cond.Operator, strValue), nil
	} else if cond.Next != nil && cond.NextLogic == "OR" {
		// 如果当前条件不匹配，但有OR逻辑，继续评估下一个条件
		return s.evaluateCondition(cond.Next, tablePrefix, tx)
	}

	return "", nil // 条件不满足
}

// executeInsert 执行INSERT语句
func (s *Server) executeInsert(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" || len(stmt.Columns) == 0 || len(stmt.Values) == 0 {
		return "", fmt.Errorf("invalid INSERT statement")
	}

	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 插入每一行数据
	for _, row := range stmt.Values {
		if len(row) != len(stmt.Columns) {
			return "", fmt.Errorf("column count doesn't match value count")
		}

		// 检查是否有id列，如果有且值为空或"auto"，则使用自增ID
		for i, col := range stmt.Columns {
			if strings.ToLower(col) == "id" {
				if row[i] == "" || strings.ToLower(row[i]) == "auto" {
					// 获取下一个自增ID
					nextID := s.db.GetNextID(stmt.Table)
					// 更新行中的ID值
					row[i] = strconv.Itoa(nextID)
				}
				break
			}
		}

		// 插入每个列的值
		for i, col := range stmt.Columns {
			key := fmt.Sprintf("%s%s", tablePrefix, col)
			value := []byte(row[i])
			if err := tx.Put(key, value); err != nil {
				return "", err
			}
		}
	}

	return fmt.Sprintf("INSERT successful: %d rows affected", len(stmt.Values)), nil
}

// executeUpdate 执行UPDATE语句
func (s *Server) executeUpdate(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" {
		return "", fmt.Errorf("table name is required")
	}

	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 简化实现，仅支持简单的等值更新
	if stmt.Where != nil {
		key := fmt.Sprintf("%s%s", tablePrefix, stmt.Where.Column)

		// 检查记录是否存在
		_, err := tx.Get(key)
		if err != nil {
			return "0 rows affected", nil
		}

		// 更新指定列
		for i, col := range stmt.Columns {
			updateKey := fmt.Sprintf("%s%s", tablePrefix, col)
			updateValue := []byte(stmt.Values[0][i])
			if err := tx.Put(updateKey, updateValue); err != nil {
				return "", err
			}
		}

		return "1 row affected", nil
	} else {
		// 全表更新 (实际实现应该使用迭代器或范围查询)
		return "UPDATE without WHERE not implemented for safety", nil
	}
}

// executeDelete 执行DELETE语句
func (s *Server) executeDelete(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" {
		return "", fmt.Errorf("table name is required")
	}

	// 构建表前缀
	tablePrefix := fmt.Sprintf("%s:", stmt.Table)

	// 如果有WHERE条件，执行条件删除
	if stmt.Where != nil {
		key := fmt.Sprintf("%s%s", tablePrefix, stmt.Where.Column)

		// 检查记录是否存在
		_, err := tx.Get(key)
		if err != nil {
			return "0 rows affected", nil
		}

		// 删除记录
		if err := tx.Delete(key); err != nil {
			return "", err
		}

		return "1 row affected", nil
	} else {
		// 全表删除 (实际实现应该使用迭代器或范围查询)
		return "DELETE without WHERE not implemented for safety", nil
	}
}

// executeCreateTable 执行CREATE TABLE语句
func (s *Server) executeCreateTable(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" {
		return "", fmt.Errorf("table name is required")
	}

	// 构建表元数据键
	metaKey := fmt.Sprintf("meta:%s", stmt.Table)

	// 检查表是否已存在
	_, err := tx.Get(metaKey)
	if err == nil {
		return "", fmt.Errorf("table '%s' already exists", stmt.Table)
	}

	// 创建表元数据
	columns := strings.Join(stmt.Columns, ",")
	if err := tx.Put(metaKey, []byte(columns)); err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' created successfully", stmt.Table), nil
}

// executeCreateIndex 执行CREATE INDEX语句
func (s *Server) executeCreateIndex(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" || stmt.IndexName == "" || len(stmt.Columns) == 0 {
		return "", fmt.Errorf("invalid CREATE INDEX statement")
	}

	// 创建索引
	if err := s.db.CreateIndex(stmt.Table, stmt.IndexName, stmt.Columns, stmt.IndexType, stmt.Unique); err != nil {
		return "", err
	}

	return fmt.Sprintf("Index '%s' created successfully on table '%s'", stmt.IndexName, stmt.Table), nil
}

// executeDropIndex 执行DROP INDEX语句
func (s *Server) executeDropIndex(stmt *parser.Statement, tx *transaction.Transaction) (string, error) {
	if stmt.Table == "" || stmt.IndexName == "" {
		return "", fmt.Errorf("invalid DROP INDEX statement")
	}

	// 删除索引
	if err := s.db.DropIndex(stmt.Table, stmt.IndexName); err != nil {
		return "", err
	}

	return fmt.Sprintf("Index '%s' dropped successfully from table '%s'", stmt.IndexName, stmt.Table), nil
}
