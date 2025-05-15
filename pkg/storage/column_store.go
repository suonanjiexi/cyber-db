package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"
	"time"
)

// ColumnStore 列式存储引擎
// 主要用于OLAP场景，提供高效的分析查询能力
type ColumnStore struct {
	// 列数据映射，格式为：dbName -> tableName -> columnName -> []value
	columnData sync.Map

	// 行ID索引，用于关联同一行的不同列
	// 格式为：dbName -> tableName -> rowID -> map[string]int (columnName -> valueIndex)
	rowIndex sync.Map

	// 列元数据，记录列的属性信息
	// 格式为：dbName -> tableName -> columnName -> ColumnMeta
	columnMeta sync.Map

	// 下一个可用的行ID
	nextRowID sync.Map

	// 访问锁
	mu sync.RWMutex
}

// ColumnMeta 列元数据
type ColumnMeta struct {
	Name       string     // 列名
	Type       ColumnType // 列类型
	IsNullable bool       // 是否可空
	Stats      ColumnStats
}

// ColumnStats 列统计信息
type ColumnStats struct {
	MinValue      interface{} // 最小值
	MaxValue      interface{} // 最大值
	DistinctCount int         // 不同值的数量
	NullCount     int         // NULL值的数量
	UpdateTime    time.Time   // 最后更新时间
}

// ColumnBatch 列数据批处理，用于高效读取和写入列数据
type ColumnBatch struct {
	ColumnName string
	Data       []interface{}
	RowIDs     []int64
}

// ColumnCompression 列压缩类型
type ColumnCompression int

const (
	// CompNone 无压缩
	CompNone ColumnCompression = iota
	// CompRLE 行程编码压缩
	CompRLE
	// CompDictionary 字典压缩
	CompDictionary
	// CompDelta 增量压缩
	CompDelta
)

// CompressedColumn 压缩后的列数据
type CompressedColumn struct {
	Data        []byte            // 压缩后的数据
	Type        ColumnType        // 列类型
	Compression ColumnCompression // 压缩类型
	Count       int               // 元素数量
	// 对于字典编码，保存字典
	Dictionary []interface{}
}

// VectorizedBatch 向量化批处理
type VectorizedBatch struct {
	Capacity  int                      // 批容量
	RowCount  int                      // 行数量
	Columns   map[string][]interface{} // 列名到列数据的映射
	Selection []int                    // 选择向量，用于过滤
}

// NewColumnStore 创建新的列式存储引擎
func NewColumnStore() *ColumnStore {
	return &ColumnStore{}
}

// 获取或创建表的列数据存储
func (cs *ColumnStore) getOrCreateColumnData(dbName, tableName, columnName string) []interface{} {
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
	columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)

	// 查找数据库级别映射
	dbValue, _ := cs.columnData.LoadOrStore(dbKey, &sync.Map{})
	dbMap := dbValue.(*sync.Map)

	// 查找表级别映射
	tableValue, _ := dbMap.LoadOrStore(tableKey, &sync.Map{})
	tableMap := tableValue.(*sync.Map)

	// 查找或创建列数据数组
	columnValue, _ := tableMap.LoadOrStore(columnKey, make([]interface{}, 0))
	columnArray, ok := columnValue.([]interface{})
	if !ok {
		// 类型断言失败，创建新数组
		columnArray = make([]interface{}, 0)
		tableMap.Store(columnKey, columnArray)
	}

	return columnArray
}

// 获取或创建表的行索引
func (cs *ColumnStore) getOrCreateRowIndex(dbName, tableName string) *sync.Map {
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	// 查找数据库级别映射
	dbValue, _ := cs.rowIndex.LoadOrStore(dbKey, &sync.Map{})
	dbMap := dbValue.(*sync.Map)

	// 查找或创建表级别行索引
	tableValue, _ := dbMap.LoadOrStore(tableKey, &sync.Map{})
	tableRowIndex := tableValue.(*sync.Map)

	return tableRowIndex
}

// 获取下一个可用的行ID
func (cs *ColumnStore) getNextRowID(dbName, tableName string) int64 {
	key := fmt.Sprintf("%s.%s", dbName, tableName)
	value, _ := cs.nextRowID.LoadOrStore(key, int64(0))
	rowID := value.(int64)
	cs.nextRowID.Store(key, rowID+1)
	return rowID
}

// CreateColumnTable 创建列式表
func (cs *ColumnStore) CreateColumnTable(ctx context.Context, dbName, tableName string, schema *TableSchema) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 为每个列创建存储
	for _, colDef := range schema.Columns {
		// 创建列元数据
		meta := ColumnMeta{
			Name:       colDef.Name,
			Type:       colDef.Type,
			IsNullable: colDef.Nullable,
			Stats: ColumnStats{
				UpdateTime: time.Now(),
			},
		}

		// 存储列元数据
		dbKey := dbName
		tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
		columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, colDef.Name)

		// 查找数据库级别映射
		dbValue, _ := cs.columnMeta.LoadOrStore(dbKey, &sync.Map{})
		dbMap := dbValue.(*sync.Map)

		// 查找表级别映射
		tableValue, _ := dbMap.LoadOrStore(tableKey, &sync.Map{})
		tableMap := tableValue.(*sync.Map)

		// 存储列元数据
		tableMap.Store(columnKey, meta)

		// 初始化列数据存储
		cs.getOrCreateColumnData(dbName, tableName, colDef.Name)
	}

	return nil
}

// InsertRow 插入一行数据
func (cs *ColumnStore) InsertRow(ctx context.Context, dbName, tableName string, values map[string]interface{}) (int64, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 获取表的行索引
	rowIndex := cs.getOrCreateRowIndex(dbName, tableName)

	// 获取新行ID
	rowID := cs.getNextRowID(dbName, tableName)

	// 创建行索引条目
	rowIndexEntry := make(map[string]int)

	// 为每个列插入值
	for columnName, value := range values {
		// 获取列数据
		columnData := cs.getOrCreateColumnData(dbName, tableName, columnName)

		// 添加值到列数据
		index := len(columnData)
		columnData = append(columnData, value)

		// 更新列数据
		dbKey := dbName
		tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
		columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)

		// 查找数据库级别映射
		dbValue, _ := cs.columnData.LoadOrStore(dbKey, &sync.Map{})
		dbMap := dbValue.(*sync.Map)

		// 查找表级别映射
		tableValue, _ := dbMap.LoadOrStore(tableKey, &sync.Map{})
		tableMap := tableValue.(*sync.Map)

		// 更新列数据
		tableMap.Store(columnKey, columnData)

		// 记录值在列中的索引
		rowIndexEntry[columnName] = index
	}

	// 存储行索引
	rowIndex.Store(rowID, rowIndexEntry)

	return rowID, nil
}

// GetRowValue 获取一行的特定列的值
func (cs *ColumnStore) GetRowValue(ctx context.Context, dbName, tableName string, rowID int64, columnName string) (interface{}, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// 获取表的行索引
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	// 查找数据库级别映射
	dbValue, ok := cs.rowIndex.Load(dbKey)
	if !ok {
		return nil, fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别行索引
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	tableRowIndex := tableValue.(*sync.Map)

	// 查找行索引条目
	rowIndexValue, ok := tableRowIndex.Load(rowID)
	if !ok {
		return nil, fmt.Errorf("行ID %d 不存在", rowID)
	}
	rowIndexEntry := rowIndexValue.(map[string]int)

	// 获取值在列中的索引
	valueIndex, ok := rowIndexEntry[columnName]
	if !ok {
		return nil, fmt.Errorf("列 %s 不存在于行ID %d 中", columnName, rowID)
	}

	// 获取列数据
	columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)

	// 查找数据库级别映射
	dbValue, ok = cs.columnData.Load(dbKey)
	if !ok {
		return nil, fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap = dbValue.(*sync.Map)

	// 查找表级别映射
	tableValue, ok = dbMap.Load(tableKey)
	if !ok {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	tableMap := tableValue.(*sync.Map)

	// 查找列数据
	columnValue, ok := tableMap.Load(columnKey)
	if !ok {
		return nil, fmt.Errorf("列 %s 不存在", columnName)
	}
	columnData := columnValue.([]interface{})

	// 返回值
	if valueIndex >= len(columnData) {
		return nil, fmt.Errorf("列数据索引越界")
	}

	return columnData[valueIndex], nil
}

// ScanColumn 扫描一个列的所有值
func (cs *ColumnStore) ScanColumn(ctx context.Context, dbName, tableName, columnName string) ([]interface{}, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// 获取列数据
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
	columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)

	// 查找数据库级别映射
	dbValue, ok := cs.columnData.Load(dbKey)
	if !ok {
		return nil, fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别映射
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	tableMap := tableValue.(*sync.Map)

	// 查找列数据
	columnValue, ok := tableMap.Load(columnKey)
	if !ok {
		return nil, fmt.Errorf("列 %s 不存在", columnName)
	}
	columnData := columnValue.([]interface{})

	// 返回列数据的副本
	result := make([]interface{}, len(columnData))
	copy(result, columnData)

	return result, nil
}

// UpdateRow 更新一行数据
func (cs *ColumnStore) UpdateRow(ctx context.Context, dbName, tableName string, rowID int64, values map[string]interface{}) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 获取表的行索引
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	// 查找数据库级别映射
	dbValue, ok := cs.rowIndex.Load(dbKey)
	if !ok {
		return fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别行索引
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return fmt.Errorf("表 %s 不存在", tableName)
	}
	tableRowIndex := tableValue.(*sync.Map)

	// 查找行索引条目
	rowIndexValue, ok := tableRowIndex.Load(rowID)
	if !ok {
		return fmt.Errorf("行ID %d 不存在", rowID)
	}
	rowIndexEntry := rowIndexValue.(map[string]int)

	// 更新每个列的值
	for columnName, value := range values {
		// 获取值在列中的索引
		valueIndex, ok := rowIndexEntry[columnName]
		if !ok {
			// 如果这是一个新列，则添加到行索引
			columnData := cs.getOrCreateColumnData(dbName, tableName, columnName)
			valueIndex = len(columnData)
			rowIndexEntry[columnName] = valueIndex

			// 在所有列数据中添加空值，直到当前索引
			for i := len(columnData); i < valueIndex; i++ {
				columnData = append(columnData, nil)
			}

			// 添加新值
			columnData = append(columnData, value)

			// 更新列数据
			dbValue, _ := cs.columnData.Load(dbKey)
			dbMap := dbValue.(*sync.Map)
			tableValue, _ := dbMap.Load(tableKey)
			tableMap := tableValue.(*sync.Map)
			columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)
			tableMap.Store(columnKey, columnData)
		} else {
			// 获取列数据
			columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)
			dbValue, _ := cs.columnData.Load(dbKey)
			dbMap := dbValue.(*sync.Map)
			tableValue, _ := dbMap.Load(tableKey)
			tableMap := tableValue.(*sync.Map)
			columnValue, _ := tableMap.Load(columnKey)
			columnData := columnValue.([]interface{})

			// 更新值
			if valueIndex < len(columnData) {
				columnData[valueIndex] = value
				tableMap.Store(columnKey, columnData)
			} else {
				return fmt.Errorf("列数据索引越界")
			}
		}
	}

	// 更新行索引
	tableRowIndex.Store(rowID, rowIndexEntry)

	return nil
}

// DeleteRow 删除一行数据
func (cs *ColumnStore) DeleteRow(ctx context.Context, dbName, tableName string, rowID int64) error {
	// 注意：在列式存储中，删除行通常不会立即物理删除数据
	// 而是标记为删除，或者在后续的压缩过程中删除
	// 这里我们简单实现为将值设为nil

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 获取表的行索引
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	// 查找数据库级别映射
	dbValue, ok := cs.rowIndex.Load(dbKey)
	if !ok {
		return fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别行索引
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return fmt.Errorf("表 %s 不存在", tableName)
	}
	tableRowIndex := tableValue.(*sync.Map)

	// 查找行索引条目
	rowIndexValue, ok := tableRowIndex.Load(rowID)
	if !ok {
		return fmt.Errorf("行ID %d 不存在", rowID)
	}
	rowIndexEntry := rowIndexValue.(map[string]int)

	// 将所有列的值设为nil
	for columnName, valueIndex := range rowIndexEntry {
		// 获取列数据
		columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)
		dbValue, _ := cs.columnData.Load(dbKey)
		dbMap := dbValue.(*sync.Map)
		tableValue, _ := dbMap.Load(tableKey)
		tableMap := tableValue.(*sync.Map)
		columnValue, _ := tableMap.Load(columnKey)
		columnData := columnValue.([]interface{})

		// 将值设为nil
		if valueIndex < len(columnData) {
			columnData[valueIndex] = nil
			tableMap.Store(columnKey, columnData)
		}
	}

	// 从行索引中删除
	tableRowIndex.Delete(rowID)

	return nil
}

// GetColumnNames 获取表的所有列名
func (cs *ColumnStore) GetColumnNames(ctx context.Context, dbName, tableName string) ([]string, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	// 查找数据库级别映射
	dbValue, ok := cs.columnMeta.Load(dbKey)
	if !ok {
		return nil, fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别映射
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	tableMap := tableValue.(*sync.Map)

	// 收集所有列名
	var columnNames []string
	tableMap.Range(func(key, value interface{}) bool {
		// 列键格式为：dbName.tableName.columnName
		keyStr := key.(string)
		parts := splitKey(keyStr)
		if len(parts) == 3 {
			columnNames = append(columnNames, parts[2])
		}
		return true
	})

	return columnNames, nil
}

// 辅助函数：拆分键
func splitKey(key string) []string {
	var result []string
	var current []byte

	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			result = append(result, string(current))
			current = []byte{}
		} else {
			current = append(current, key[i])
		}
	}

	if len(current) > 0 {
		result = append(result, string(current))
	}

	return result
}

// Compress 压缩列数据，移除已删除的值
func (cs *ColumnStore) Compress(ctx context.Context, dbName, tableName string) error {
	// 列式存储的压缩通常涉及到重新排列数据，消除空隙等
	// 这是一个复杂操作，这里仅实现简单版本

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 获取表的所有列
	columnNames, err := cs.GetColumnNames(ctx, dbName, tableName)
	if err != nil {
		return err
	}

	// 获取表的行索引
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)

	dbValue, ok := cs.rowIndex.Load(dbKey)
	if !ok {
		return fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return fmt.Errorf("表 %s 不存在", tableName)
	}
	tableRowIndex := tableValue.(*sync.Map)

	// 对每一列进行压缩
	for _, columnName := range columnNames {
		// 获取列数据
		columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)
		dbValue, _ := cs.columnData.Load(dbKey)
		dbMap := dbValue.(*sync.Map)
		tableValue, _ := dbMap.Load(tableKey)
		tableMap := tableValue.(*sync.Map)
		columnValue, ok := tableMap.Load(columnKey)
		if !ok {
			continue
		}
		columnData := columnValue.([]interface{})

		// 创建新的压缩列数据
		newColumnData := make([]interface{}, 0, len(columnData))
		indexMap := make(map[int]int) // 旧索引 -> 新索引

		// 收集非nil值
		for i, value := range columnData {
			if value != nil {
				indexMap[i] = len(newColumnData)
				newColumnData = append(newColumnData, value)
			}
		}

		// 更新列数据
		tableMap.Store(columnKey, newColumnData)

		// 更新所有行索引
		tableRowIndex.Range(func(key, value interface{}) bool {
			rowID := key.(int64)
			rowIndexEntry := value.(map[string]int)

			if oldIndex, ok := rowIndexEntry[columnName]; ok {
				if newIndex, ok := indexMap[oldIndex]; ok {
					rowIndexEntry[columnName] = newIndex
				} else {
					// 值已被删除，从行索引中移除
					delete(rowIndexEntry, columnName)
				}
			}

			// 更新行索引
			tableRowIndex.Store(rowID, rowIndexEntry)
			return true
		})
	}

	return nil
}

// 添加列压缩功能
func (cs *ColumnStore) CompressColumn(ctx context.Context, dbName, tableName, columnName string, compressionType ColumnCompression) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// 获取原始列数据
	columnData, err := cs.getColumnData(dbName, tableName, columnName)
	if err != nil {
		return err
	}

	if len(columnData) == 0 {
		return nil // 空列，无需压缩
	}

	var compressedData CompressedColumn
	compressedData.Type = cs.getColumnType(dbName, tableName, columnName)
	compressedData.Compression = compressionType
	compressedData.Count = len(columnData)

	switch compressionType {
	case CompNone:
		// 无压缩，直接存储
		return nil

	case CompRLE:
		// 行程编码压缩
		compressed, err := cs.compressRLE(columnData)
		if err != nil {
			return err
		}
		compressedData.Data = compressed

	case CompDictionary:
		// 字典压缩
		compressed, dictionary, err := cs.compressDictionary(columnData)
		if err != nil {
			return err
		}
		compressedData.Data = compressed
		compressedData.Dictionary = dictionary

	case CompDelta:
		// 增量压缩
		compressed, err := cs.compressDelta(columnData)
		if err != nil {
			return err
		}
		compressedData.Data = compressed

	default:
		return fmt.Errorf("不支持的压缩类型: %d", compressionType)
	}

	// 保存压缩数据到元数据
	metaKey := fmt.Sprintf("%s.%s.%s.compression", dbName, tableName, columnName)

	// 获取数据库级映射
	dbKey := dbName
	dbValue, _ := cs.columnMeta.Load(dbKey)
	dbMap := dbValue.(*sync.Map)

	// 查找表级映射
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
	tableValue, _ := dbMap.Load(tableKey)
	tableMap := tableValue.(*sync.Map)

	// 存储压缩元数据
	tableMap.Store(metaKey, compressedData)

	return nil
}

// 行程编码压缩
func (cs *ColumnStore) compressRLE(data []interface{}) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var result []byte
	var count int = 1
	var current = data[0]

	for i := 1; i < len(data); i++ {
		if data[i] == current {
			count++
		} else {
			// 编码当前值和计数
			valueBytes, err := encodeValue(current)
			if err != nil {
				return nil, err
			}

			countBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(countBytes, uint32(count))

			result = append(result, countBytes...)
			result = append(result, valueBytes...)

			// 重置
			current = data[i]
			count = 1
		}
	}

	// 处理最后一组
	valueBytes, err := encodeValue(current)
	if err != nil {
		return nil, err
	}

	countBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, uint32(count))

	result = append(result, countBytes...)
	result = append(result, valueBytes...)

	return result, nil
}

// 字典压缩
func (cs *ColumnStore) compressDictionary(data []interface{}) ([]byte, []interface{}, error) {
	if len(data) == 0 {
		return nil, nil, nil
	}

	// 创建字典
	dict := make(map[interface{}]int)
	var dictionary []interface{}

	// 第一遍：构建字典
	for _, v := range data {
		if _, exists := dict[v]; !exists {
			dict[v] = len(dictionary)
			dictionary = append(dictionary, v)
		}
	}

	// 第二遍：使用字典索引替换值
	result := make([]byte, len(data)*4) // 每个索引4字节
	for i, v := range data {
		idx := dict[v]
		binary.LittleEndian.PutUint32(result[i*4:], uint32(idx))
	}

	return result, dictionary, nil
}

// 增量压缩（适用于整数类型）
func (cs *ColumnStore) compressDelta(data []interface{}) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// 检查是否为数值类型
	firstVal, ok := data[0].(int64)
	if !ok {
		return nil, fmt.Errorf("增量编码仅支持整数类型")
	}

	// 首先存储基准值
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, uint64(firstVal))

	// 然后存储差值
	for i := 1; i < len(data); i++ {
		val, ok := data[i].(int64)
		if !ok {
			return nil, fmt.Errorf("数据类型不一致")
		}

		// 计算差值
		delta := val - firstVal
		firstVal = val

		// 使用变长编码存储差值
		var deltaBytes []byte
		if delta == 0 {
			deltaBytes = []byte{0}
		} else if delta >= -63 && delta <= 64 {
			// 小差值用一个字节
			deltaBytes = []byte{byte(delta & 0x7F)}
		} else if delta >= -8191 && delta <= 8192 {
			// 中等差值用两个字节
			deltaBytes = make([]byte, 2)
			deltaBytes[0] = byte(0x80 | ((delta >> 8) & 0x3F))
			deltaBytes[1] = byte(delta & 0xFF)
		} else {
			// 大差值用9个字节（1字节标记 + 8字节值）
			deltaBytes = make([]byte, 9)
			deltaBytes[0] = 0xC0
			binary.LittleEndian.PutUint64(deltaBytes[1:], uint64(delta))
		}

		result = append(result, deltaBytes...)
	}

	return result, nil
}

// 编码单个值
func encodeValue(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 获取列数据
func (cs *ColumnStore) getColumnData(dbName, tableName, columnName string) ([]interface{}, error) {
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
	columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)

	// 查找数据库级别映射
	dbValue, ok := cs.columnData.Load(dbKey)
	if !ok {
		return nil, fmt.Errorf("数据库 %s 不存在", dbName)
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别映射
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	tableMap := tableValue.(*sync.Map)

	// 查找列数据
	columnValue, ok := tableMap.Load(columnKey)
	if !ok {
		return nil, fmt.Errorf("列 %s 不存在", columnName)
	}

	columnArray, ok := columnValue.([]interface{})
	if !ok {
		return nil, fmt.Errorf("列类型断言失败")
	}

	return columnArray, nil
}

// 获取列类型
func (cs *ColumnStore) getColumnType(dbName, tableName, columnName string) ColumnType {
	dbKey := dbName
	tableKey := fmt.Sprintf("%s.%s", dbName, tableName)
	columnKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, columnName)

	// 查找数据库级别映射
	dbValue, ok := cs.columnMeta.Load(dbKey)
	if !ok {
		return TypeString // 默认为字符串类型
	}
	dbMap := dbValue.(*sync.Map)

	// 查找表级别映射
	tableValue, ok := dbMap.Load(tableKey)
	if !ok {
		return TypeString
	}
	tableMap := tableValue.(*sync.Map)

	// 查找列元数据
	metaValue, ok := tableMap.Load(columnKey)
	if !ok {
		return TypeString
	}

	meta, ok := metaValue.(ColumnMeta)
	if !ok {
		return TypeString
	}

	return meta.Type
}

// VectorizedScan 向量化扫描列数据
func (cs *ColumnStore) VectorizedScan(ctx context.Context, dbName, tableName string, columns []string, batchSize int) (*VectorizedBatch, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	batch := NewVectorizedBatch(batchSize)

	// 获取表中所有列
	if len(columns) == 0 {
		var err error
		columns, err = cs.GetColumnNames(ctx, dbName, tableName)
		if err != nil {
			return nil, err
		}
	}

	// 为每个列分配空间
	for _, colName := range columns {
		colData, err := cs.getColumnData(dbName, tableName, colName)
		if err != nil {
			return nil, err
		}

		if len(colData) == 0 {
			continue
		}

		// 确定要读取的行数
		rowCount := min(batchSize, len(colData))
		batch.RowCount = rowCount

		// 分配列数据
		batch.Columns[colName] = make([]interface{}, rowCount)

		// 复制数据
		copy(batch.Columns[colName], colData[:rowCount])
	}

	return batch, nil
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// NewVectorizedBatch 创建新的向量化批处理
func NewVectorizedBatch(capacity int) *VectorizedBatch {
	return &VectorizedBatch{
		Capacity:  capacity,
		RowCount:  0,
		Columns:   make(map[string][]interface{}),
		Selection: nil,
	}
}
