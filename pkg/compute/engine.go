package compute

import (
	"context"
)

// Engine 计算引擎接口
type Engine interface {
	// Execute 执行SQL语句
	Execute(ctx context.Context, sql string) (*Result, error)

	// CreateSession 创建新会话
	CreateSession() *Session

	// Close 关闭计算引擎
	Close() error
}

// ColumnType 列类型
type ColumnType int

const (
	// TypeInt 整型
	TypeInt ColumnType = iota
	// TypeFloat 浮点型
	TypeFloat
	// TypeString 字符串型
	TypeString
	// TypeBytes 字节型
	TypeBytes
	// TypeBool 布尔型
	TypeBool
	// TypeTimestamp 时间戳型
	TypeTimestamp
)

// ColumnInfo 列信息
type ColumnInfo struct {
	// Name 列名
	Name string
	// Type 列类型
	Type ColumnType
	// Table 所属表
	Table string
	// Database 所属数据库
	Database string
}

// RowValue 行数据
type RowValue []interface{}

// Result 查询结果
type Result struct {
	// IsQuery 是否是查询语句
	IsQuery bool

	// Columns 列信息
	Columns []ColumnInfo

	// Rows 行数据
	Rows []RowValue

	// AffectedRows 受影响的行数
	AffectedRows int64

	// LastInsertID 最后插入的ID
	LastInsertID int64
}

// Session 会话
type Session struct {
	engine Engine
	ctx    context.Context
	cancel context.CancelFunc

	// 会话变量
	variables map[string]string
}

// NewSession 创建会话
func NewSession(engine Engine) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	return &Session{
		engine:    engine,
		ctx:       ctx,
		cancel:    cancel,
		variables: make(map[string]string),
	}
}

// Execute 在会话中执行SQL
func (s *Session) Execute(sql string) (*Result, error) {
	return s.engine.Execute(s.ctx, sql)
}

// SetVariable 设置会话变量
func (s *Session) SetVariable(name, value string) {
	s.variables[name] = value
}

// GetVariable 获取会话变量
func (s *Session) GetVariable(name string) (string, bool) {
	value, exists := s.variables[name]
	return value, exists
}

// Close 关闭会话
func (s *Session) Close() {
	s.cancel()
}
