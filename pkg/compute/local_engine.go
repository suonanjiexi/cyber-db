package compute

import (
	"context"
	"fmt"
	"sync"

	"github.com/cyberdb/cyberdb/pkg/parser"
	"github.com/cyberdb/cyberdb/pkg/storage"
)

// LocalEngine 本地计算引擎实现
type LocalEngine struct {
	// storageEngine 存储引擎
	storageEngine storage.Engine

	// currentDB 当前数据库
	currentDB string
	mu        sync.RWMutex
}

// NewLocalEngine 创建新的本地计算引擎
func NewLocalEngine(storageEngine storage.Engine) *LocalEngine {
	return &LocalEngine{
		storageEngine: storageEngine,
	}
}

// CreateSession 创建新会话
func (e *LocalEngine) CreateSession() *Session {
	return NewSession(e)
}

// Close 关闭计算引擎
func (e *LocalEngine) Close() error {
	return nil
}

// Execute 执行SQL语句
func (e *LocalEngine) Execute(ctx context.Context, sql string) (*Result, error) {
	// 解析SQL语句
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("解析SQL失败: %w", err)
	}

	// 根据语句类型执行不同操作
	switch stmt.Type() {
	case parser.StatementUse:
		return e.executeUse(ctx, stmt.(*parser.UseStmt))
	case parser.StatementShow:
		return e.executeShow(ctx, stmt.(*parser.ShowStmt))
	case parser.StatementCreate:
		if createDBStmt, ok := stmt.(*parser.CreateDatabaseStmt); ok {
			return e.executeCreateDatabase(ctx, createDBStmt)
		} else if createTableStmt, ok := stmt.(*parser.CreateTableStmt); ok {
			return e.executeCreateTable(ctx, createTableStmt)
		}
		return nil, fmt.Errorf("不支持的CREATE语句")
	case parser.StatementDrop:
		if dropDBStmt, ok := stmt.(*parser.DropDatabaseStmt); ok {
			return e.executeDropDatabase(ctx, dropDBStmt)
		} else if dropTableStmt, ok := stmt.(*parser.DropTableStmt); ok {
			return e.executeDropTable(ctx, dropTableStmt)
		}
		return nil, fmt.Errorf("不支持的DROP语句")
	case parser.StatementSelect:
		return e.executeSelect(ctx, stmt.(*parser.SelectStmt))
	case parser.StatementInsert:
		return e.executeInsert(ctx, stmt.(*parser.InsertStmt))
	case parser.StatementUpdate:
		return e.executeUpdate(ctx, stmt.(*parser.UpdateStmt))
	case parser.StatementDelete:
		return e.executeDelete(ctx, stmt.(*parser.DeleteStmt))
	default:
		return nil, fmt.Errorf("不支持的SQL语句类型")
	}
}

// executeUse 执行USE语句
func (e *LocalEngine) executeUse(ctx context.Context, stmt *parser.UseStmt) (*Result, error) {
	e.mu.Lock()
	e.currentDB = stmt.Database
	e.mu.Unlock()

	// USE语句返回成功结果
	return &Result{
		IsQuery:      false,
		AffectedRows: 0,
		LastInsertID: 0,
	}, nil
}

// executeShow 执行SHOW语句
func (e *LocalEngine) executeShow(ctx context.Context, stmt *parser.ShowStmt) (*Result, error) {
	// 简化版实现，实际应该从存储引擎中获取数据库或表信息
	switch stmt.Object {
	case "DATABASES":
		// 示例数据，实际应该从存储引擎获取
		return &Result{
			IsQuery: true,
			Columns: []ColumnInfo{
				{Name: "Database", Type: TypeString},
			},
			Rows: []RowValue{
				{"mysql"},
				{"information_schema"},
				{"test"},
			},
		}, nil
	case "TABLES":
		// 检查当前数据库是否设置
		e.mu.RLock()
		currentDB := e.currentDB
		e.mu.RUnlock()

		if currentDB == "" {
			return nil, fmt.Errorf("未选择数据库")
		}

		// 示例数据，实际应该从存储引擎获取
		return &Result{
			IsQuery: true,
			Columns: []ColumnInfo{
				{Name: "Tables_in_" + currentDB, Type: TypeString},
			},
			Rows: []RowValue{
				{"users"},
				{"products"},
			},
		}, nil
	default:
		return nil, fmt.Errorf("不支持的SHOW命令: %s", stmt.Object)
	}
}

// executeCreateDatabase 执行CREATE DATABASE语句
func (e *LocalEngine) executeCreateDatabase(ctx context.Context, stmt *parser.CreateDatabaseStmt) (*Result, error) {
	err := e.storageEngine.CreateDB(ctx, stmt.Database)
	if err != nil {
		// 如果设置了IF NOT EXISTS并且错误是数据库已存在，则忽略错误
		if stmt.IfNotExists && err.Error() == fmt.Sprintf("数据库 %s 已存在", stmt.Database) {
			return &Result{
				IsQuery:      false,
				AffectedRows: 0,
				LastInsertID: 0,
			}, nil
		}
		return nil, err
	}

	return &Result{
		IsQuery:      false,
		AffectedRows: 1,
		LastInsertID: 0,
	}, nil
}

// convertToColumnDefinitions 将解析器的列定义转换为存储引擎的列定义
func convertToColumnDefinitions(columns []parser.ColumnDef) []storage.ColumnDefinition {
	result := make([]storage.ColumnDefinition, len(columns))

	for i, col := range columns {
		var colType storage.ColumnType

		// 简化的类型转换，实际实现需要更全面的映射
		switch col.Type {
		case "INT":
			colType = storage.TypeInt
		case "FLOAT":
			colType = storage.TypeFloat
		case "VARCHAR", "TEXT", "CHAR":
			colType = storage.TypeString
		case "BLOB", "BINARY":
			colType = storage.TypeBytes
		case "BOOL", "BOOLEAN":
			colType = storage.TypeBool
		case "TIMESTAMP", "DATETIME":
			colType = storage.TypeTimestamp
		default:
			colType = storage.TypeString // 默认为字符串类型
		}

		result[i] = storage.ColumnDefinition{
			Name:     col.Name,
			Type:     colType,
			Nullable: !col.NotNull,
			Primary:  col.PrimaryKey,
			Unique:   col.PrimaryKey, // 主键默认唯一
			Default:  nil,            // 简化处理，暂不支持默认值
		}
	}

	return result
}

// executeCreateTable 执行CREATE TABLE语句
func (e *LocalEngine) executeCreateTable(ctx context.Context, stmt *parser.CreateTableStmt) (*Result, error) {
	// 确定数据库名
	dbName := stmt.Database
	if dbName == "" {
		e.mu.RLock()
		dbName = e.currentDB
		e.mu.RUnlock()
		if dbName == "" {
			return nil, fmt.Errorf("未选择数据库")
		}
	}

	// 构建表结构
	schema := &storage.TableSchema{
		Columns: convertToColumnDefinitions(stmt.Columns),
		Indexes: []storage.IndexDefinition{}, // 简化处理，暂不支持索引
	}

	// 调用存储引擎创建表
	err := e.storageEngine.CreateTable(ctx, dbName, stmt.Table, schema)
	if err != nil {
		// 如果设置了IF NOT EXISTS并且错误是表已存在，则忽略错误
		if stmt.IfNotExists && err.Error() == fmt.Sprintf("表 %s 已存在", stmt.Table) {
			return &Result{
				IsQuery:      false,
				AffectedRows: 0,
				LastInsertID: 0,
			}, nil
		}
		return nil, err
	}

	return &Result{
		IsQuery:      false,
		AffectedRows: 1,
		LastInsertID: 0,
	}, nil
}

// executeDropDatabase 执行DROP DATABASE语句
func (e *LocalEngine) executeDropDatabase(ctx context.Context, stmt *parser.DropDatabaseStmt) (*Result, error) {
	err := e.storageEngine.DropDB(ctx, stmt.Database)
	if err != nil {
		// 如果设置了IF EXISTS并且错误是数据库不存在，则忽略错误
		if stmt.IfExists && err == storage.ErrDBNotFound {
			return &Result{
				IsQuery:      false,
				AffectedRows: 0,
				LastInsertID: 0,
			}, nil
		}
		return nil, err
	}

	return &Result{
		IsQuery:      false,
		AffectedRows: 1,
		LastInsertID: 0,
	}, nil
}

// executeDropTable 执行DROP TABLE语句
func (e *LocalEngine) executeDropTable(ctx context.Context, stmt *parser.DropTableStmt) (*Result, error) {
	// 确定数据库名
	dbName := stmt.Database
	if dbName == "" {
		e.mu.RLock()
		dbName = e.currentDB
		e.mu.RUnlock()
		if dbName == "" {
			return nil, fmt.Errorf("未选择数据库")
		}
	}

	err := e.storageEngine.DropTable(ctx, dbName, stmt.Table)
	if err != nil {
		// 如果设置了IF EXISTS并且错误是表不存在，则忽略错误
		if stmt.IfExists && err == storage.ErrTableNotFound {
			return &Result{
				IsQuery:      false,
				AffectedRows: 0,
				LastInsertID: 0,
			}, nil
		}
		return nil, err
	}

	return &Result{
		IsQuery:      false,
		AffectedRows: 1,
		LastInsertID: 0,
	}, nil
}

// executeSelect 执行SELECT语句
func (e *LocalEngine) executeSelect(ctx context.Context, stmt *parser.SelectStmt) (*Result, error) {
	// 确定数据库名
	e.mu.RLock()
	dbName := e.currentDB
	e.mu.RUnlock()

	if dbName == "" {
		return nil, fmt.Errorf("未选择数据库")
	}

	// 简化的SELECT实现，仅支持单表无条件查询
	if len(stmt.Tables) != 1 {
		return nil, fmt.Errorf("不支持多表查询或空表查询")
	}

	tableName := stmt.Tables[0]

	// 从存储引擎中扫描数据
	iterator, err := e.storageEngine.Scan(ctx, dbName, tableName, nil, nil)
	if err != nil {
		return nil, err
	}
	defer iterator.Close()

	// 收集结果
	rows := make([]RowValue, 0)
	for iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()

		// 简化的反序列化，实际应该根据表结构进行处理
		rows = append(rows, RowValue{string(key), string(value)})
	}

	if err := iterator.Error(); err != nil {
		return nil, err
	}

	// 构建结果
	result := &Result{
		IsQuery: true,
		Columns: []ColumnInfo{
			{Name: "key", Type: TypeString, Table: tableName, Database: dbName},
			{Name: "value", Type: TypeString, Table: tableName, Database: dbName},
		},
		Rows: rows,
	}

	return result, nil
}

// executeInsert 执行INSERT语句
func (e *LocalEngine) executeInsert(ctx context.Context, stmt *parser.InsertStmt) (*Result, error) {
	// 确定数据库名
	dbName := stmt.Database
	if dbName == "" {
		e.mu.RLock()
		dbName = e.currentDB
		e.mu.RUnlock()
		if dbName == "" {
			return nil, fmt.Errorf("未选择数据库")
		}
	}

	// 简化的插入实现，仅支持单行简单插入
	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("无数据插入")
	}

	// 开始事务
	tx, err := e.storageEngine.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	// 确保事务最终提交或回滚
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	// 插入数据
	affectedRows := int64(0)
	lastInsertID := int64(0)

	for _, row := range stmt.Values {
		if len(row) != len(stmt.Columns) {
			err = fmt.Errorf("列数与值数不匹配")
			return nil, err
		}

		// 假设第一列为ID
		if len(row) > 0 {
			if id, ok := row[0].(int64); ok && id > lastInsertID {
				lastInsertID = id
			}
		}

		// 简化的序列化，实际应该根据表结构进行处理
		// 这里假设第一列为主键
		key := fmt.Sprintf("%v", row[0])
		value := fmt.Sprintf("%v", row)

		if err = tx.Put(ctx, dbName, stmt.Table, []byte(key), []byte(value)); err != nil {
			return nil, err
		}

		affectedRows++
	}

	// 提交事务
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	// 构建结果
	result := &Result{
		IsQuery:      false,
		AffectedRows: affectedRows,
		LastInsertID: lastInsertID,
	}

	return result, nil
}

// executeUpdate 执行UPDATE语句
func (e *LocalEngine) executeUpdate(ctx context.Context, stmt *parser.UpdateStmt) (*Result, error) {
	// 确定数据库名
	dbName := stmt.Database
	if dbName == "" {
		e.mu.RLock()
		dbName = e.currentDB
		e.mu.RUnlock()
		if dbName == "" {
			return nil, fmt.Errorf("未选择数据库")
		}
	}

	// 简化的更新实现，仅支持通过 ID = x 条件更新单行
	if len(stmt.Conditions) != 1 || stmt.Conditions[0].Field != "id" || stmt.Conditions[0].Operator != "=" {
		return nil, fmt.Errorf("仅支持通过ID更新单行")
	}

	// 开始事务
	tx, err := e.storageEngine.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	// 确保事务最终提交或回滚
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	// 获取要更新的行
	key := fmt.Sprintf("%v", stmt.Conditions[0].Value)
	data, err := tx.Get(ctx, dbName, stmt.Table, []byte(key))
	if err != nil {
		return nil, err
	}

	// 简化的反序列化和更新，实际应该根据表结构进行处理
	// 这里仅演示更新逻辑
	newValue := string(data) // 复制原始值，然后更新

	// 更新数据并写回
	if err = tx.Put(ctx, dbName, stmt.Table, []byte(key), []byte(newValue)); err != nil {
		return nil, err
	}

	// 提交事务
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	// 构建结果
	result := &Result{
		IsQuery:      false,
		AffectedRows: 1,
		LastInsertID: 0,
	}

	return result, nil
}

// executeDelete 执行DELETE语句
func (e *LocalEngine) executeDelete(ctx context.Context, stmt *parser.DeleteStmt) (*Result, error) {
	// 确定数据库名
	dbName := stmt.Database
	if dbName == "" {
		e.mu.RLock()
		dbName = e.currentDB
		e.mu.RUnlock()
		if dbName == "" {
			return nil, fmt.Errorf("未选择数据库")
		}
	}

	// 简化的删除实现，仅支持通过 ID = x 条件删除单行
	if len(stmt.Conditions) != 1 || stmt.Conditions[0].Field != "id" || stmt.Conditions[0].Operator != "=" {
		return nil, fmt.Errorf("仅支持通过ID删除单行")
	}

	// 开始事务
	tx, err := e.storageEngine.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	// 确保事务最终提交或回滚
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	// 删除数据
	key := fmt.Sprintf("%v", stmt.Conditions[0].Value)
	if err = tx.Delete(ctx, dbName, stmt.Table, []byte(key)); err != nil {
		return nil, err
	}

	// 提交事务
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	// 构建结果
	result := &Result{
		IsQuery:      false,
		AffectedRows: 1,
		LastInsertID: 0,
	}

	return result, nil
}
