package transaction

import (
	"errors"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/suonanjiexi/cyber-db/pkg/storage"
)

// 定义错误类型
var (
	ErrTxNotActive = errors.New("transaction not active")
	ErrTxTimeout   = errors.New("transaction timeout")
	ErrTxConflict  = errors.New("transaction conflict")
	ErrDeadlock    = errors.New("transaction deadlock detected")
)

// 事务状态常量
const (
	TxStatusActive = iota
	TxStatusCommitted
	TxStatusAborted
)

// 事务隔离级别常量
const (
	ReadUncommitted = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// Transaction 表示一个数据库事务
type Transaction struct {
	id             string              // 事务ID
	status         int                 // 事务状态
	startTime      time.Time           // 事务开始时间
	timeout        time.Duration       // 事务超时时间
	db             *storage.DB         // 底层数据库
	writeSet       map[string][]byte   // 写集合
	readSet        map[string]struct{} // 读集合
	mutex          sync.RWMutex        // 读写锁
	isolationLevel int                 // 事务隔离级别
	readCache      map[string][]byte   // 读缓存，避免重复读取相同的键
	statistics     *TxStats            // 事务统计信息
	priority       int                 // 事务优先级，用于死锁预防
	dependencies   map[string]struct{} // 事务依赖关系，用于死锁检测
}

// TxStats 事务统计信息
type TxStats struct {
	ReadCount      int           // 读操作次数
	WriteCount     int           // 写操作次数
	Conflicts      int           // 冲突次数
	Retries        int           // 重试次数
	TotalReadTime  time.Duration // 总读取时间
	TotalWriteTime time.Duration // 总写入时间
	mutex          sync.Mutex    // 统计信息锁
}

// Manager 事务管理器
type Manager struct {
	db                 *storage.DB
	activeTransactions map[string]*Transaction
	mutex              sync.RWMutex
	defaultTimeout     time.Duration
	defaultIsolation   int
	lockManager        *LockManager
	cleanupTicker      *time.Ticker               // 用于定期清理超时事务
	statistics         *ManagerStats              // 事务管理器统计信息
	shardCount         int                        // 分片数量，用于细粒度锁控制
	shardMutexes       []sync.RWMutex             // 分片锁数组
	deadlockDetector   *DeadlockDetector          // 死锁检测器
	resultCache        *lru.Cache[string, []byte] // 查询结果缓存
	monitorTicker      *time.Ticker               // 监控定时器
	slowTxThreshold    time.Duration              // 慢事务阈值
	activeAlerts       map[string]time.Time       // 活跃告警
	alertMutex         sync.Mutex                 // 告警互斥锁
}

// ManagerStats 事务管理器统计信息
type ManagerStats struct {
	TxStarted     int64         // 启动的事务数
	TxCommitted   int64         // 提交的事务数
	TxAborted     int64         // 中止的事务数
	TxTimeout     int64         // 超时的事务数
	Deadlocks     int64         // 检测到的死锁数
	AvgTxDuration time.Duration // 平均事务持续时间
	mutex         sync.Mutex    // 统计信息锁
}

// LockType 锁类型
type LockType int

const (
	ReadLock LockType = iota
	WriteLock
)

// LockManager 负责管理事务锁
type LockManager struct {
	locks        map[string]*LockEntry          // 键 -> 锁条目
	waitForGraph map[string]map[string]struct{} // 等待图，用于死锁检测
	mutex        sync.Mutex                     // 互斥锁
	timeout      time.Duration                  // 锁超时时间
}

// LockEntry 表示单个资源上的锁
type LockEntry struct {
	key         string              // 被锁定的键
	readLocks   map[string]struct{} // 持有读锁的事务ID
	writeLock   string              // 持有写锁的事务ID (空表示无写锁)
	waitingList []*LockRequest      // 等待此锁的请求队列
	mutex       sync.Mutex          // 保护此锁条目的互斥锁
	// 兼容旧代码的字段
	Type      LockType  // 锁类型
	Holder    string    // 持有锁的事务ID
	WaitQueue []string  // 等待队列（事务ID列表）
	Timestamp time.Time // 获取锁的时间戳
}

// LockRequest 表示锁请求
type LockRequest struct {
	TxID        string    // 事务ID
	Key         string    // 请求锁定的键
	Type        LockType  // 锁类型
	Deadline    time.Time // 请求超时时间
	grantedChan chan bool // 通知锁授予的通道
	startTime   time.Time // 请求开始时间
}

// TransactionManager 是Manager的别名，用于兼容现有代码
type TransactionManager = Manager

// 分布式事务协议类型
type TransactionProtocol int

const (
	// TwoPhaseCommit 两阶段提交协议
	TwoPhaseCommit TransactionProtocol = iota
	// ThreePhaseCommit 三阶段提交协议
	ThreePhaseCommit
)

// TransactionState 事务状态
type TransactionState string

const (
	// 两阶段提交状态
	TxInitial    TransactionState = "initial"
	TxPreparing  TransactionState = "preparing"
	TxPrepared   TransactionState = "prepared"
	TxCommitting TransactionState = "committing"
	TxCommitted  TransactionState = "committed"
	TxAborting   TransactionState = "aborting"
	TxAborted    TransactionState = "aborted"

	// 三阶段提交额外状态
	TxCanCommit TransactionState = "can_commit"
	TxPreCommit TransactionState = "pre_commit"
)

// generateTxID 生成事务ID
func generateTxID() string {
	return fmt.Sprintf("tx-%d-%d", time.Now().UnixNano(), time.Now().Unix()%1000)
}
