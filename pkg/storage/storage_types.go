package storage

import (
	"bytes"
	"errors"
	"sync"
	"time"

	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

// 定义错误类型
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDBNotOpen   = errors.New("database not open")
)

// IndexManager 管理数据库中的所有索引
type IndexManager struct {
	indexes     map[string]*Index // 索引名称 -> 索引
	db          *DB               // 数据库引用
	mutex       sync.RWMutex      // 读写锁
	columnCache sync.Map          // 表列 -> 索引的缓存
}

// Index 表示一个索引
type Index struct {
	Config     IndexConfig                        // 索引配置
	Root       *BPlusTreeNode                     // B+树根节点
	mutex      sync.RWMutex                       // 读写锁
	Degree     int                                // B+树的度
	Height     int                                // 树的高度
	Size       int                                // 索引中的键数量
	NodeCache  *lru.Cache[string, *BPlusTreeNode] // 节点缓存
	LastAccess time.Time                          // 最后访问时间
	Stats      *IndexStats                        // 索引统计信息
	HashIndex  interface{}                        // 哈希索引实现
}

// IndexConfig 表示索引配置
type IndexConfig struct {
	Name    string   // 索引名称
	Table   string   // 表名
	Columns []string // 索引列
	Type    int      // 索引类型
	Unique  bool     // 是否唯一索引
}

// BPlusTreeNode B+树节点
type BPlusTreeNode struct {
	IsLeaf   bool             // 是否是叶子节点
	Keys     []string         // 键值
	Children []*BPlusTreeNode // 子节点（非叶子节点）
	Values   [][]string       // 记录ID列表（叶子节点）
	Next     *BPlusTreeNode   // 下一个叶子节点的指针（叶子节点）
	Prev     *BPlusTreeNode   // 上一个叶子节点的指针（叶子节点，用于反向遍历）
	Parent   *BPlusTreeNode   // 父节点指针（用于快速向上遍历）
	Height   int              // 节点高度（根节点为0）
	Mutex    sync.RWMutex     // 节点级锁，提高并发性能
	Dirty    bool             // 标记节点是否被修改
}

// IndexStats 索引统计信息
type IndexStats struct {
	Lookups            int64         // 查找操作次数
	RangeLookups       int64         // 范围查找操作次数
	Inserts            int64         // 插入操作次数
	Deletes            int64         // 删除操作次数
	Splits             int64         // 节点分裂次数
	Merges             int64         // 节点合并次数
	CacheHits          int64         // 缓存命中次数
	CacheMisses        int64         // 缓存未命中次数
	AvgLookupTime      time.Duration // 平均查找时间
	MaxLookupTime      time.Duration // 最大查找时间
	MinLookupTime      time.Duration // 最小查找时间
	AvgInsertTime      time.Duration // 平均插入时间
	MaxInsertTime      time.Duration // 最大插入时间
	MinInsertTime      time.Duration // 最小插入时间
	AvgRangeLookupTime time.Duration // 平均范围查找时间
	MaxRangeLookupTime time.Duration // 最大范围查找时间
	AvgRangeSize       int           // 平均范围大小
	Resizes            int64         // 哈希表调整大小次数
	LastResizeTime     time.Duration // 最后一次调整大小耗时
	AvgBucketSize      float64       // 平均桶大小
	MaxBucketSize      int           // 最大桶大小
	Mutex              sync.Mutex    // 统计信息锁
}

// DB 表示一个简单的键值存储数据库
// 在DB结构体中添加以下字段
type DB struct {
	path          string                     // 数据库文件路径
	isOpen        bool                       // 数据库是否打开
	data          map[string][]byte          // 内存中的数据
	cache         *lru.Cache[string, []byte] // LRU缓存
	mutex         sync.RWMutex               // 读写锁，保证并发安全
	persistent    bool                       // 是否持久化存储
	counters      map[string]int             // 自增ID计数器
	counterMutex  sync.RWMutex               // 计数器的互斥锁
	charset       string                     // 字符集编码，默认为utf8mb4
	indexManager  *IndexManager              // 索引管理器
	batchSize     int                        // 批量写入大小
	batchBuffer   []writeOp                  // 批量写入缓冲区
	batchMutex    sync.Mutex                 // 批量写入互斥锁
	flushTicker   *time.Ticker               // 定期刷新计时器
	flushDone     chan struct{}              // 用于停止刷新goroutine的通道
	preReadSize   int                        // 预读取大小
	preReadBuffer *sync.Map                  // 预读取缓冲区
	writeBuffer   *bytes.Buffer              // 写入缓冲区
	compression   bool                       // 是否启用压缩
	memoryPool    sync.Pool                  // 内存池
	shardCount    int                        // 分片数量，用于细粒度锁控制
	shardMutexes  []sync.RWMutex             // 分片锁数组
	resultCache   *lru.Cache[string, []byte] // 查询结果缓存

	// 统计信息字段
	cacheHits         int64                // 缓存命中次数
	cacheMisses       int64                // 缓存未命中次数
	getOps            int64                // Get操作次数
	putOps            int64                // Put操作次数
	deleteOps         int64                // Delete操作次数
	avgGetTime        time.Duration        // 平均Get操作时间
	avgPutTime        time.Duration        // 平均Put操作时间
	lastFlushTime     time.Time            // 最后一次刷新时间
	startTime         time.Time            // 数据库启动时间
	modificationTimes map[string]time.Time // 键的最后修改时间
	lastBackupTime    time.Time            // 最后备份时间
	backupTicker      *time.Ticker         // 定期备份计时器
	backupKeepCount   int                  // 保留的备份数量
}

// writeOp 表示一个写操作
type writeOp struct {
	key   string
	value []byte
}

// DBStats 数据库统计信息
type DBStats struct {
	KeyCount       int           // 键值对数量
	TotalDataSize  int64         // 总数据大小（字节）
	CacheHitRate   float64       // 缓存命中率
	CacheHits      int64         // 缓存命中次数
	CacheMisses    int64         // 缓存未命中次数
	GetOps         int64         // Get操作次数
	PutOps         int64         // Put操作次数
	DeleteOps      int64         // Delete操作次数
	AvgGetTime     time.Duration // 平均Get操作时间
	AvgPutTime     time.Duration // 平均Put操作时间
	IndexCount     int           // 索引数量
	LastFlushTime  time.Time     // 最后一次刷新时间
	LastBackupTime time.Time     // 最后一次备份时间
	StartTime      time.Time     // 数据库启动时间
	Uptime         time.Duration // 运行时间
}
