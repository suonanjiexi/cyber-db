package storage

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// 定义索引类型
const (
	BPlusTreeIndex = iota // B+树索引
	HashIndex             // 哈希索引
)

// 定义索引错误
var (
	ErrIndexExists      = errors.New("index already exists")
	ErrIndexNotFound    = errors.New("index not found")
	ErrInvalidIndex     = errors.New("invalid index configuration")
	ErrInvalidIndexType = errors.New("invalid index type")
)

// GetIndexManager 返回数据库的索引管理器
func (db *DB) GetIndexManager() *IndexManager {
	return db.indexManager
}

// CreateIndex 创建索引
func (db *DB) CreateIndex(table, name string, columns []string, indexType int, unique bool) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	config := IndexConfig{
		Name:    name,
		Table:   table,
		Columns: columns,
		Type:    indexType,
		Unique:  unique,
	}

	return db.indexManager.CreateIndex(config)
}

// DropIndex 删除索引
func (db *DB) DropIndex(table, name string) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	return db.indexManager.DropIndex(table, name)
}

// NewIndexManager 创建一个新的索引管理器
func NewIndexManager(db *DB) *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
		db:      db,
	}
}

// CreateIndex 创建一个新索引
func (im *IndexManager) CreateIndex(config IndexConfig) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 验证索引配置
	if config.Name == "" || config.Table == "" || len(config.Columns) == 0 {
		return ErrInvalidIndex
	}

	// 检查索引类型是否有效
	if config.Type != BPlusTreeIndex && config.Type != HashIndex {
		return ErrInvalidIndexType
	}

	// 检查索引是否已存在
	indexKey := fmt.Sprintf("%s:%s", config.Table, config.Name)
	if _, exists := im.indexes[indexKey]; exists {
		return ErrIndexExists
	}

	// 创建新索引
	index := &Index{
		Config: config,
		Root:   nil,
		Degree: 4, // 默认B+树度为4
		mutex:  sync.RWMutex{},
	}

	// 初始化节点缓存
	nodeCache, err := lru.New[string, *BPlusTreeNode](1000) // 默认缓存1000个节点
	if err != nil {
		return fmt.Errorf("failed to create node cache: %w", err)
	}
	index.NodeCache = nodeCache

	// 初始化统计信息
	index.Stats = &IndexStats{}

	// 初始化最后访问时间
	index.LastAccess = time.Now()

	// 初始化B+树根节点
	if config.Type == BPlusTreeIndex {
		index.Root = &BPlusTreeNode{
			IsLeaf: true,
			Keys:   make([]string, 0),
			Values: make([][]string, 0),
		}
	}

	// 将索引添加到管理器
	im.indexes[indexKey] = index

	// 构建索引
	err = im.buildIndex(index)
	if err != nil {
		delete(im.indexes, indexKey)
		return err
	}

	// 持久化索引元数据
	return im.saveIndexMetadata(index)
}

// buildIndex 为现有数据构建索引
func (im *IndexManager) buildIndex(index *Index) error {
	// 获取表的所有数据
	tablePrefix := fmt.Sprintf("%s:", index.Config.Table)
	keys, err := im.db.PrefixScan(tablePrefix)
	if err != nil {
		return fmt.Errorf("failed to scan table data: %w", err)
	}

	// 将数据按记录ID分组
	recordMap := make(map[string]map[string]interface{})
	for _, key := range keys {
		parts := strings.SplitN(key[len(tablePrefix):], ":", 2)
		if len(parts) != 2 {
			continue
		}

		recordID := parts[0]
		columnName := parts[1]

		// 获取值
		value, err := im.db.Get(key)
		if err != nil {
			continue
		}

		// 将值添加到记录
		if _, exists := recordMap[recordID]; !exists {
			recordMap[recordID] = make(map[string]interface{})
		}
		recordMap[recordID][columnName] = string(value)
	}

	// 为每条记录创建索引
	for recordID, recordData := range recordMap {
		// 提取索引键
		indexKey, err := im.extractIndexKey(&Record{ID: recordID, Data: recordData}, index.Config.Columns)
		if err != nil {
			continue
		}

		// 将记录ID添加到索引
		if index.Config.Type == BPlusTreeIndex {
			im.insertIntoBPlusTree(index, indexKey, recordID)
		} else if index.Config.Type == HashIndex {
			// 添加到哈希索引的逻辑
		}
	}

	return nil
}

// Record 表示一条记录
type Record struct {
	ID   string
	Data map[string]interface{}
}

// extractIndexKey 从记录中提取索引键
func (im *IndexManager) extractIndexKey(record *Record, columns []string) (string, error) {
	if len(columns) == 1 {
		// 单列索引
		value, ok := record.Data[columns[0]]
		if !ok {
			return "", fmt.Errorf("记录中不存在列 %s", columns[0])
		}

		// 将值转换为字符串
		switch v := value.(type) {
		case string:
			return v, nil
		case int, int32, int64, float32, float64, bool:
			return fmt.Sprintf("%v", v), nil
		default:
			return "", fmt.Errorf("不支持的索引列类型: %T", v)
		}
	} else {
		// 复合索引
		var keyParts []string
		for _, col := range columns {
			value, ok := record.Data[col]
			if !ok {
				return "", fmt.Errorf("记录中不存在列 %s", col)
			}

			// 将值转换为字符串
			switch v := value.(type) {
			case string:
				keyParts = append(keyParts, v)
			case int, int32, int64, float32, float64, bool:
				keyParts = append(keyParts, fmt.Sprintf("%v", v))
			default:
				return "", fmt.Errorf("不支持的索引列类型: %T", v)
			}
		}

		// 使用特殊分隔符连接复合键
		return strings.Join(keyParts, "\x00"), nil
	}
}

// saveIndexMetadata 保存索引元数据
func (im *IndexManager) saveIndexMetadata(index *Index) error {
	// 构建索引元数据键
	metaKey := fmt.Sprintf("index:%s:%s", index.Config.Table, index.Config.Name)

	// 序列化索引配置
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(index.Config); err != nil {
		return err
	}

	// 保存到数据库
	return im.db.Put(metaKey, buf.Bytes())
}

// DropIndex 删除索引
func (im *IndexManager) DropIndex(table, name string) error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 构建索引键
	indexKey := fmt.Sprintf("%s:%s", table, name)

	// 检查索引是否存在
	if _, exists := im.indexes[indexKey]; !exists {
		return ErrIndexNotFound
	}

	// 从管理器中删除索引
	delete(im.indexes, indexKey)

	// 删除索引元数据
	metaKey := fmt.Sprintf("index:%s:%s", table, name)
	return im.db.Delete(metaKey)
}

// GetIndex 通过表名和索引名获取索引
func (im *IndexManager) GetIndex(table, name string) (*Index, error) {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	// 构建索引键
	indexKey := fmt.Sprintf("%s:%s", table, name)

	// 查找索引
	index, exists := im.indexes[indexKey]
	if !exists {
		return nil, ErrIndexNotFound
	}

	// 更新最后访问时间
	index.LastAccess = time.Now()

	return index, nil
}

// GetTableIndex 通过表名和列名获取索引
func (im *IndexManager) GetTableIndex(table, column string) *Index {
	// 先检查缓存
	cacheKey := indexCacheKey{table: table, column: column}
	if cachedIndex, ok := im.columnCache.Load(cacheKey); ok {
		index := cachedIndex.(*Index)
		// 更新最后访问时间
		index.LastAccess = time.Now()
		return index
	}

	im.mutex.RLock()
	defer im.mutex.RUnlock()

	// 遍历所有索引，查找包含指定列的索引
	for _, index := range im.indexes {
		if index.Config.Table == table {
			// 检查索引是否包含指定列
			for _, col := range index.Config.Columns {
				if col == column {
					// 更新最后访问时间
					index.LastAccess = time.Now()
					// 添加到缓存
					im.columnCache.Store(cacheKey, index)
					return index
				}
			}
		}
	}

	return nil
}

// 索引缓存键
type indexCacheKey struct {
	table  string
	column string
}

// insertIntoBPlusTree 将记录ID插入B+树索引
func (im *IndexManager) insertIntoBPlusTree(index *Index, key string, recordID string) error {
	// 获取根节点
	root := index.Root
	if root == nil {
		return fmt.Errorf("B+树索引未初始化")
	}

	// 在叶子节点中找到插入位置
	leaf := im.findLeafNode(root, key)

	// 检查是否为唯一索引
	if index.Config.Unique {
		// 检查键是否已存在
		for _, k := range leaf.Keys {
			if k == key {
				return fmt.Errorf("唯一索引违反约束: %s", key)
			}
		}
	}

	// 在叶子节点中插入键和值
	insertPos := 0
	for insertPos < len(leaf.Keys) && leaf.Keys[insertPos] < key {
		insertPos++
	}

	// 如果键已存在，将记录ID添加到值列表
	if insertPos < len(leaf.Keys) && leaf.Keys[insertPos] == key {
		// 检查记录ID是否已存在
		for _, id := range leaf.Values[insertPos] {
			if id == recordID {
				return nil // 记录ID已存在，不需要再次添加
			}
		}
		leaf.Values[insertPos] = append(leaf.Values[insertPos], recordID)
	} else {
		// 插入新键和值
		leaf.Keys = append(leaf.Keys, "")
		copy(leaf.Keys[insertPos+1:], leaf.Keys[insertPos:])
		leaf.Keys[insertPos] = key

		leaf.Values = append(leaf.Values, nil)
		copy(leaf.Values[insertPos+1:], leaf.Values[insertPos:])
		leaf.Values[insertPos] = []string{recordID}
	}

	// 如果叶子节点超过度的上限，分裂节点
	if len(leaf.Keys) > 2*index.Degree {
		im.splitLeafNode(index, leaf)
	}

	return nil
}

// findLeafNode 找到键应该插入的叶子节点
func (im *IndexManager) findLeafNode(node *BPlusTreeNode, key string) *BPlusTreeNode {
	if node.IsLeaf {
		return node
	}

	// 在内部节点中查找下一个子节点
	pos := 0
	for pos < len(node.Keys) && key >= node.Keys[pos] {
		pos++
	}

	// 如果pos超出范围，使用最后一个子节点
	if pos >= len(node.Children) {
		pos = len(node.Children) - 1
	}

	return im.findLeafNode(node.Children[pos], key)
}

// splitLeafNode 分裂叶子节点
func (im *IndexManager) splitLeafNode(index *Index, leaf *BPlusTreeNode) {
	// 分裂点
	mid := len(leaf.Keys) / 2

	// 创建新的叶子节点
	newLeaf := &BPlusTreeNode{
		IsLeaf: true,
		Keys:   make([]string, len(leaf.Keys)-mid),
		Values: make([][]string, len(leaf.Values)-mid),
	}

	// 复制后半部分到新节点
	copy(newLeaf.Keys, leaf.Keys[mid:])
	copy(newLeaf.Values, leaf.Values[mid:])

	// 更新原节点
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]

	// 更新链表指针
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf
	newLeaf.Prev = leaf

	if newLeaf.Next != nil {
		newLeaf.Next.Prev = newLeaf
	}

	// 将分裂的键插入父节点
	splitKey := newLeaf.Keys[0]
	im.insertIntoParent(index, leaf, splitKey, newLeaf)

	// 更新统计信息
	if index.Stats != nil {
		index.Stats.Splits++
	}
}

// insertIntoParent 将键和右子节点插入到父节点
func (im *IndexManager) insertIntoParent(index *Index, left *BPlusTreeNode, key string, right *BPlusTreeNode) {
	// 如果左节点是根节点，创建新的根节点
	if left.Parent == nil {
		newRoot := &BPlusTreeNode{
			IsLeaf:   false,
			Keys:     []string{key},
			Children: []*BPlusTreeNode{left, right},
		}
		index.Root = newRoot
		left.Parent = newRoot
		right.Parent = newRoot
		index.Height++
		return
	}

	// 获取父节点
	parent := left.Parent
	right.Parent = parent

	// 找到左节点在父节点中的位置
	pos := 0
	for pos < len(parent.Children) && parent.Children[pos] != left {
		pos++
	}

	// 在父节点中插入键和右子节点
	parent.Keys = append(parent.Keys, "")
	copy(parent.Keys[pos+1:], parent.Keys[pos:])
	parent.Keys[pos] = key

	parent.Children = append(parent.Children, nil)
	copy(parent.Children[pos+2:], parent.Children[pos+1:])
	parent.Children[pos+1] = right

	// 如果父节点超过度的上限，分裂内部节点
	if len(parent.Keys) > 2*index.Degree {
		im.splitInternalNode(index, parent)
	}
}

// splitInternalNode 分裂内部节点
func (im *IndexManager) splitInternalNode(index *Index, node *BPlusTreeNode) {
	// 分裂点
	mid := len(node.Keys) / 2

	// 创建新的内部节点
	newNode := &BPlusTreeNode{
		IsLeaf:   false,
		Keys:     make([]string, len(node.Keys)-mid-1),
		Children: make([]*BPlusTreeNode, len(node.Children)-mid),
	}

	// 复制键和子节点到新节点
	copy(newNode.Keys, node.Keys[mid+1:])
	copy(newNode.Children, node.Children[mid+1:])

	// 更新子节点的父指针
	for _, child := range newNode.Children {
		child.Parent = newNode
	}

	// 获取中间键
	midKey := node.Keys[mid]

	// 更新原节点
	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]

	// 将中间键和新节点插入到父节点
	im.insertIntoParent(index, node, midKey, newNode)
}

// LoadIndexes 从数据库加载所有索引
func (im *IndexManager) LoadIndexes() error {
	im.mutex.Lock()
	defer im.mutex.Unlock()

	// 清空现有索引
	im.indexes = make(map[string]*Index)

	// 使用前缀扫描查找所有索引元数据
	prefix := "index:"
	keys, err := im.db.PrefixScan(prefix)
	if err != nil {
		return fmt.Errorf("扫描索引元数据失败: %w", err)
	}

	// 并行加载索引以提高性能
	var wg sync.WaitGroup
	indexChan := make(chan *Index, len(keys))
	errChan := make(chan error, len(keys))

	for _, key := range keys {
		wg.Add(1)
		go func(metaKey string) {
			defer wg.Done()

			// 获取索引元数据
			data, err := im.db.Get(metaKey)
			if err != nil {
				errChan <- fmt.Errorf("获取索引元数据失败 %s: %w", metaKey, err)
				return
			}

			// 解码索引配置
			var config IndexConfig
			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			if err := dec.Decode(&config); err != nil {
				errChan <- fmt.Errorf("解码索引配置失败 %s: %w", metaKey, err)
				return
			}

			// 创建索引实例
			index := &Index{
				Config: config,
				Degree: 4, // 默认B+树度为4
				mutex:  sync.RWMutex{},
			}

			// 初始化节点缓存
			nodeCache, err := lru.New[string, *BPlusTreeNode](1000)
			if err != nil {
				errChan <- fmt.Errorf("创建节点缓存失败: %w", err)
				return
			}
			index.NodeCache = nodeCache

			// 初始化统计信息
			index.Stats = &IndexStats{}

			// 初始化最后访问时间
			index.LastAccess = time.Now()

			// 初始化B+树根节点
			if config.Type == BPlusTreeIndex {
				index.Root = &BPlusTreeNode{
					IsLeaf: true,
					Keys:   make([]string, 0),
					Values: make([][]string, 0),
				}
			} else if config.Type == HashIndex {
				// 初始化哈希索引
				// 这里应该根据实际情况初始化哈希索引
			}

			// 将索引添加到通道
			indexChan <- index
		}(key)
	}

	// 等待所有索引加载完成
	wg.Wait()
	close(indexChan)
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		return err
	}

	// 将加载的索引添加到管理器
	for index := range indexChan {
		indexKey := fmt.Sprintf("%s:%s", index.Config.Table, index.Config.Name)
		im.indexes[indexKey] = index
	}

	log.Printf("成功加载 %d 个索引", len(im.indexes))
	return nil
}

// GetAllIndexes 获取所有索引
func (im *IndexManager) GetAllIndexes() []*Index {
	im.mutex.RLock()
	defer im.mutex.RUnlock()

	indexes := make([]*Index, 0, len(im.indexes))
	for _, index := range im.indexes {
		indexes = append(indexes, index)
	}

	return indexes
}

// PrefixScan 方法扫描指定前缀的所有键
func (db *DB) PrefixScan(prefix string) ([]string, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return nil, ErrDBNotOpen
	}

	// 收集匹配前缀的键
	var keys []string
	for k := range db.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}
