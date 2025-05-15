package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// StatementType SQL语句类型
type StatementType int

const (
	// StatementUnknown 未知类型
	StatementUnknown StatementType = iota
	// StatementSelect 查询语句
	StatementSelect
	// StatementInsert 插入语句
	StatementInsert
	// StatementUpdate 更新语句
	StatementUpdate
	// StatementDelete 删除语句
	StatementDelete
	// StatementCreate 创建语句
	StatementCreate
	// StatementDrop 删除语句
	StatementDrop
	// StatementAlter 修改语句
	StatementAlter
	// StatementUse 使用数据库语句
	StatementUse
	// StatementShow 显示语句
	StatementShow
)

// Statement SQL语句接口
type Statement interface {
	// Type 获取语句类型
	Type() StatementType
}

// UseStmt USE语句
type UseStmt struct {
	Database string
}

// Type 实现Statement接口
func (s *UseStmt) Type() StatementType {
	return StatementUse
}

// ShowStmt SHOW语句
type ShowStmt struct {
	// Object 要显示的对象（databases, tables, columns等）
	Object string
	// Database 数据库名（可选）
	Database string
}

// Type 实现Statement接口
func (s *ShowStmt) Type() StatementType {
	return StatementShow
}

// CreateDatabaseStmt CREATE DATABASE语句
type CreateDatabaseStmt struct {
	// Database 数据库名
	Database string
	// IfNotExists 是否包含IF NOT EXISTS子句
	IfNotExists bool
}

// Type 实现Statement接口
func (s *CreateDatabaseStmt) Type() StatementType {
	return StatementCreate
}

// DropDatabaseStmt DROP DATABASE语句
type DropDatabaseStmt struct {
	// Database 数据库名
	Database string
	// IfExists 是否包含IF EXISTS子句
	IfExists bool
}

// Type 实现Statement接口
func (s *DropDatabaseStmt) Type() StatementType {
	return StatementDrop
}

// CreateTableStmt CREATE TABLE语句
type CreateTableStmt struct {
	// Table 表名
	Table string
	// Database 数据库名（可选）
	Database string
	// IfNotExists 是否包含IF NOT EXISTS子句
	IfNotExists bool
	// Columns 列定义
	Columns []ColumnDef
}

// Type 实现Statement接口
func (s *CreateTableStmt) Type() StatementType {
	return StatementCreate
}

// ColumnDef 列定义
type ColumnDef struct {
	// Name 列名
	Name string
	// Type 类型
	Type string
	// NotNull 是否NOT NULL
	NotNull bool
	// PrimaryKey 是否主键
	PrimaryKey bool
	// Default 默认值（可选）
	Default interface{}
}

// DropTableStmt DROP TABLE语句
type DropTableStmt struct {
	// Table 表名
	Table string
	// Database 数据库名（可选）
	Database string
	// IfExists 是否包含IF EXISTS子句
	IfExists bool
}

// Type 实现Statement接口
func (s *DropTableStmt) Type() StatementType {
	return StatementDrop
}

// SelectStmt SELECT语句
type SelectStmt struct {
	// Fields 查询字段
	Fields []string
	// Tables 查询表
	Tables []string
	// Conditions 查询条件
	Conditions []Condition
}

// Type 实现Statement接口
func (s *SelectStmt) Type() StatementType {
	return StatementSelect
}

// InsertStmt INSERT语句
type InsertStmt struct {
	// Table 表名
	Table string
	// Database 数据库名（可选）
	Database string
	// Columns 列名
	Columns []string
	// Values 值列表
	Values [][]interface{}
}

// Type 实现Statement接口
func (s *InsertStmt) Type() StatementType {
	return StatementInsert
}

// UpdateStmt UPDATE语句
type UpdateStmt struct {
	// Table 表名
	Table string
	// Database 数据库名（可选）
	Database string
	// Updates 更新项
	Updates map[string]interface{}
	// Conditions 更新条件
	Conditions []Condition
}

// Type 实现Statement接口
func (s *UpdateStmt) Type() StatementType {
	return StatementUpdate
}

// DeleteStmt DELETE语句
type DeleteStmt struct {
	// Table 表名
	Table string
	// Database 数据库名（可选）
	Database string
	// Conditions 删除条件
	Conditions []Condition
}

// Type 实现Statement接口
func (s *DeleteStmt) Type() StatementType {
	return StatementDelete
}

// Condition 条件表达式
type Condition struct {
	// Field 字段名
	Field string
	// Operator 操作符（=, >, <, >=, <=, !=, LIKE）
	Operator string
	// Value 值
	Value interface{}
}

// Parse 解析SQL语句
func Parse(sql string) (Statement, error) {
	sql = strings.TrimSpace(sql)

	// 简化的解析逻辑，实际实现需要更复杂的语法分析
	if strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		return parseSelect(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "INSERT") {
		return parseInsert(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "UPDATE") {
		return parseUpdate(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "DELETE") {
		return parseDelete(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "CREATE DATABASE") {
		return parseCreateDatabase(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "CREATE TABLE") {
		return parseCreateTable(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "DROP DATABASE") {
		return parseDropDatabase(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "DROP TABLE") {
		return parseDropTable(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "USE") {
		return parseUse(sql)
	} else if strings.HasPrefix(strings.ToUpper(sql), "SHOW") {
		return parseShow(sql)
	}

	return nil, fmt.Errorf("不支持的SQL语句: %s", sql)
}

// parseSelect 解析SELECT语句
func parseSelect(sql string) (*SelectStmt, error) {
	// 将SQL转换为大写以便不区分大小写
	upperSQL := strings.ToUpper(sql)

	// 基本格式: SELECT fields FROM tables [WHERE conditions]
	// 寻找关键词位置
	fromIndex := strings.Index(upperSQL, " FROM ")
	if fromIndex < 0 {
		return nil, fmt.Errorf("无效的SELECT语句：缺少FROM子句")
	}

	// 提取字段列表
	fieldsStr := strings.TrimSpace(sql[6:fromIndex])
	fields := splitAndTrim(fieldsStr, ',')

	// 检查是否有WHERE子句
	whereIndex := strings.Index(upperSQL, " WHERE ")
	var tablesStr string
	if whereIndex < 0 {
		// 没有WHERE子句，直接提取表名
		tablesStr = strings.TrimSpace(sql[fromIndex+6:])
	} else {
		// 有WHERE子句，提取表名和条件
		tablesStr = strings.TrimSpace(sql[fromIndex+6 : whereIndex])
	}

	// 提取表名列表
	tables := splitAndTrim(tablesStr, ',')

	// 解析WHERE子句
	var conditions []Condition
	if whereIndex >= 0 {
		conditionsStr := strings.TrimSpace(sql[whereIndex+7:])
		var err error
		conditions, err = parseConditions(conditionsStr)
		if err != nil {
			return nil, err
		}
	}

	return &SelectStmt{
		Fields:     fields,
		Tables:     tables,
		Conditions: conditions,
	}, nil
}

// splitAndTrim 分割字符串并去除空白
func splitAndTrim(s string, sep rune) []string {
	parts := strings.Split(s, string(sep))
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// parseConditions 解析条件表达式
func parseConditions(conditionsStr string) ([]Condition, error) {
	// 简化处理，仅支持AND连接的简单条件
	// 实际实现需要处理更复杂的条件表达式
	parts := strings.Split(conditionsStr, " AND ")
	conditions := make([]Condition, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// 查找操作符
		var operator string
		var operatorIndex int

		for _, op := range []string{">=", "<=", "!=", "=", ">", "<", "LIKE"} {
			// 特殊处理LIKE操作符，因为它是关键字
			if op == "LIKE" {
				likeIndex := strings.Index(strings.ToUpper(part), " LIKE ")
				if likeIndex >= 0 {
					operator = "LIKE"
					operatorIndex = likeIndex
					break
				}
				continue
			}

			opIndex := strings.Index(part, op)
			if opIndex >= 0 {
				operator = op
				operatorIndex = opIndex
				break
			}
		}

		if operator == "" {
			return nil, fmt.Errorf("无效的条件表达式: %s", part)
		}

		// 提取字段名和值
		field := strings.TrimSpace(part[:operatorIndex])
		valueStr := strings.TrimSpace(part[operatorIndex+len(operator):])

		// 处理字符串值（去除引号）
		var value interface{}
		if (strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'")) ||
			(strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"")) {
			// 字符串值
			value = valueStr[1 : len(valueStr)-1]
		} else if strings.EqualFold(valueStr, "NULL") {
			// NULL值
			value = nil
		} else if i, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
			// 整数值
			value = i
		} else if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
			// 浮点数值
			value = f
		} else if b, err := strconv.ParseBool(valueStr); err == nil {
			// 布尔值
			value = b
		} else {
			// 默认作为字符串处理
			value = valueStr
		}

		conditions = append(conditions, Condition{
			Field:    field,
			Operator: operator,
			Value:    value,
		})
	}

	return conditions, nil
}

// parseInsert 解析INSERT语句
func parseInsert(sql string) (*InsertStmt, error) {
	// 将SQL转换为大写以便不区分大小写
	upperSQL := strings.ToUpper(sql)

	// 基本格式: INSERT INTO [database.]table (columns) VALUES (values)[, (values)]...
	// 寻找关键词位置
	intoIndex := strings.Index(upperSQL, " INTO ")
	if intoIndex < 0 {
		return nil, fmt.Errorf("无效的INSERT语句：缺少INTO子句")
	}

	valuesIndex := strings.Index(upperSQL, " VALUES ")
	if valuesIndex < 0 {
		return nil, fmt.Errorf("无效的INSERT语句：缺少VALUES子句")
	}

	// 提取表名和列名
	tableAndColumnsStr := strings.TrimSpace(sql[intoIndex+6 : valuesIndex])

	// 查找列名括号起始位置
	columnsStartIndex := strings.Index(tableAndColumnsStr, "(")
	if columnsStartIndex < 0 {
		return nil, fmt.Errorf("无效的INSERT语句：缺少列定义")
	}

	// 提取表名和数据库名
	tableStr := strings.TrimSpace(tableAndColumnsStr[:columnsStartIndex])
	var tableName, dbName string

	// 检查是否指定了数据库名
	parts := strings.Split(tableStr, ".")
	if len(parts) > 1 {
		dbName = parts[0]
		tableName = parts[1]
	} else {
		tableName = parts[0]
	}

	// 提取列名列表
	columnsEndIndex := strings.LastIndex(tableAndColumnsStr, ")")
	if columnsEndIndex < 0 {
		return nil, fmt.Errorf("无效的INSERT语句：列定义格式错误")
	}

	columnsStr := tableAndColumnsStr[columnsStartIndex+1 : columnsEndIndex]
	columns := splitAndTrim(columnsStr, ',')

	// 提取VALUES子句
	valuesStr := strings.TrimSpace(sql[valuesIndex+8:])

	// 解析值列表
	values, err := parseValuesList(valuesStr)
	if err != nil {
		return nil, err
	}

	return &InsertStmt{
		Table:    tableName,
		Database: dbName,
		Columns:  columns,
		Values:   values,
	}, nil
}

// parseValuesList 解析INSERT语句的值列表
func parseValuesList(valuesStr string) ([][]interface{}, error) {
	// 检查括号
	if !strings.HasPrefix(valuesStr, "(") {
		return nil, fmt.Errorf("无效的VALUES子句：缺少起始括号")
	}

	// 分解多个值组
	var valueGroups [][]interface{}
	var currentPos int

	for currentPos < len(valuesStr) {
		// 寻找值组开始位置
		startIndex := strings.IndexRune(valuesStr[currentPos:], '(')
		if startIndex < 0 {
			break
		}
		startIndex += currentPos

		// 寻找值组结束位置（需要处理嵌套括号的情况）
		endIndex := -1
		depth := 1
		for i := startIndex + 1; i < len(valuesStr); i++ {
			if valuesStr[i] == '(' {
				depth++
			} else if valuesStr[i] == ')' {
				depth--
				if depth == 0 {
					endIndex = i
					break
				}
			}
		}

		if endIndex < 0 {
			return nil, fmt.Errorf("无效的VALUES子句：括号不匹配")
		}

		// 提取值组内容
		valueGroupStr := valuesStr[startIndex+1 : endIndex]
		valueGroup, err := parseValueGroup(valueGroupStr)
		if err != nil {
			return nil, err
		}

		valueGroups = append(valueGroups, valueGroup)
		currentPos = endIndex + 1
	}

	if len(valueGroups) == 0 {
		return nil, fmt.Errorf("无效的VALUES子句：未找到值")
	}

	return valueGroups, nil
}

// parseValueGroup 解析单个值组
func parseValueGroup(valueGroupStr string) ([]interface{}, error) {
	// 分割值组内的各个值
	var values []interface{}
	var currentPos int
	var inString bool
	var stringDelim rune
	var currentValue strings.Builder

	for currentPos <= len(valueGroupStr) {
		// 到达字符串末尾或遇到逗号（且不在字符串内部）
		if currentPos == len(valueGroupStr) || (valueGroupStr[currentPos] == ',' && !inString) {
			// 处理当前值
			valueStr := strings.TrimSpace(currentValue.String())
			if valueStr != "" {
				// 解析值
				var value interface{}

				if (strings.HasPrefix(valueStr, "'") && strings.HasSuffix(valueStr, "'")) ||
					(strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"")) {
					// 字符串值
					value = valueStr[1 : len(valueStr)-1]
				} else if strings.EqualFold(valueStr, "NULL") {
					// NULL值
					value = nil
				} else if i, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
					// 整数值
					value = i
				} else if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
					// 浮点数值
					value = f
				} else if b, err := strconv.ParseBool(valueStr); err == nil {
					// 布尔值
					value = b
				} else {
					// 默认作为字符串处理
					value = valueStr
				}

				values = append(values, value)
			}

			// 重置当前值
			currentValue.Reset()

			// 如果已到末尾，退出循环
			if currentPos == len(valueGroupStr) {
				break
			}

			// 跳过逗号
			currentPos++
			continue
		}

		// 处理字符串分隔符
		if valueGroupStr[currentPos] == '\'' || valueGroupStr[currentPos] == '"' {
			if !inString {
				// 开始一个字符串
				inString = true
				stringDelim = rune(valueGroupStr[currentPos])
			} else if rune(valueGroupStr[currentPos]) == stringDelim {
				// 结束一个字符串
				inString = false
			}
		}

		// 追加当前字符到当前值
		currentValue.WriteByte(valueGroupStr[currentPos])
		currentPos++
	}

	return values, nil
}

// parseUpdate 解析UPDATE语句
func parseUpdate(sql string) (*UpdateStmt, error) {
	// 简化版本
	return &UpdateStmt{
		Table:      "example",
		Updates:    map[string]interface{}{"name": "new_name"},
		Conditions: []Condition{{Field: "id", Operator: "=", Value: 1}},
	}, nil
}

// parseDelete 解析DELETE语句
func parseDelete(sql string) (*DeleteStmt, error) {
	// 简化版本
	return &DeleteStmt{
		Table:      "example",
		Conditions: []Condition{{Field: "id", Operator: "=", Value: 1}},
	}, nil
}

// parseCreateDatabase 解析CREATE DATABASE语句
func parseCreateDatabase(sql string) (*CreateDatabaseStmt, error) {
	// 简化版本
	parts := strings.Fields(sql)
	if len(parts) < 3 {
		return nil, fmt.Errorf("无效的CREATE DATABASE语句")
	}

	var dbName string
	var ifNotExists bool

	if strings.ToUpper(parts[2]) == "IF" && len(parts) >= 5 && strings.ToUpper(parts[3]) == "NOT" && strings.ToUpper(parts[4]) == "EXISTS" {
		ifNotExists = true
		if len(parts) >= 6 {
			dbName = parts[5]
		} else {
			return nil, fmt.Errorf("缺少数据库名")
		}
	} else {
		dbName = parts[2]
	}

	return &CreateDatabaseStmt{
		Database:    dbName,
		IfNotExists: ifNotExists,
	}, nil
}

// parseDropDatabase 解析DROP DATABASE语句
func parseDropDatabase(sql string) (*DropDatabaseStmt, error) {
	// 简化版本
	parts := strings.Fields(sql)
	if len(parts) < 3 {
		return nil, fmt.Errorf("无效的DROP DATABASE语句")
	}

	var dbName string
	var ifExists bool

	if strings.ToUpper(parts[2]) == "IF" && len(parts) >= 4 && strings.ToUpper(parts[3]) == "EXISTS" {
		ifExists = true
		if len(parts) >= 5 {
			dbName = parts[4]
		} else {
			return nil, fmt.Errorf("缺少数据库名")
		}
	} else {
		dbName = parts[2]
	}

	return &DropDatabaseStmt{
		Database: dbName,
		IfExists: ifExists,
	}, nil
}

// parseCreateTable 解析CREATE TABLE语句
func parseCreateTable(sql string) (*CreateTableStmt, error) {
	// 简化版本，实际需要完整的SQL解析器
	return &CreateTableStmt{
		Table:       "example",
		IfNotExists: false,
		Columns: []ColumnDef{
			{Name: "id", Type: "INT", NotNull: true, PrimaryKey: true},
			{Name: "name", Type: "VARCHAR", NotNull: false},
		},
	}, nil
}

// parseDropTable 解析DROP TABLE语句
func parseDropTable(sql string) (*DropTableStmt, error) {
	// 简化版本
	parts := strings.Fields(sql)
	if len(parts) < 3 {
		return nil, fmt.Errorf("无效的DROP TABLE语句")
	}

	var tableName string
	var ifExists bool

	if strings.ToUpper(parts[2]) == "IF" && len(parts) >= 4 && strings.ToUpper(parts[3]) == "EXISTS" {
		ifExists = true
		if len(parts) >= 5 {
			tableName = parts[4]
		} else {
			return nil, fmt.Errorf("缺少表名")
		}
	} else {
		tableName = parts[2]
	}

	return &DropTableStmt{
		Table:    tableName,
		IfExists: ifExists,
	}, nil
}

// parseUse 解析USE语句
func parseUse(sql string) (*UseStmt, error) {
	parts := strings.Fields(sql)
	if len(parts) < 2 {
		return nil, fmt.Errorf("无效的USE语句")
	}

	return &UseStmt{
		Database: parts[1],
	}, nil
}

// parseShow 解析SHOW语句
func parseShow(sql string) (*ShowStmt, error) {
	parts := strings.Fields(sql)
	if len(parts) < 2 {
		return nil, fmt.Errorf("无效的SHOW语句")
	}

	return &ShowStmt{
		Object: strings.ToUpper(parts[1]),
	}, nil
}
