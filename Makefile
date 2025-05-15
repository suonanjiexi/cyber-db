# CyberDB Makefile

# 变量定义
GO = go
GOFMT = gofmt
GOFILES = $(shell find . -name "*.go" -type f)
BINDIR = bin
SERVER_BIN = $(BINDIR)/cyberdb
CLIENT_BIN = $(BINDIR)/cyberdb-client
TOOLS_BIN = $(BINDIR)/cyberdb-tools
CONFIG_DIR = configs
SINGLE_NODE_CONFIG = $(CONFIG_DIR)/single-node.yaml
MASTER_CONFIG = $(CONFIG_DIR)/cluster-master.yaml
SLAVE_CONFIG = $(CONFIG_DIR)/cluster-slave.yaml
HTAP_CONFIG = $(CONFIG_DIR)/htap.yaml

# 默认目标：构建所有
.PHONY: all
all: clean fmt build

# 格式化代码
.PHONY: fmt
fmt:
	@echo "格式化Go代码..."
	@$(GOFMT) -w $(GOFILES)

# 清理
.PHONY: clean
clean:
	@echo "清理构建目录..."
	@rm -rf $(BINDIR)
	@mkdir -p $(BINDIR)

# 构建服务器和客户端
.PHONY: build
build: build-server build-client build-tools

# 构建服务器
.PHONY: build-server
build-server:
	@echo "构建CyberDB服务器..."
	@$(GO) build -o $(SERVER_BIN) ./cmd/server

# 构建客户端
.PHONY: build-client
build-client:
	@echo "构建CyberDB客户端..."
	@$(GO) build -o $(CLIENT_BIN) ./cmd/client

# 构建工具集
.PHONY: build-tools
build-tools:
	@echo "构建CyberDB工具集..."
	@$(GO) build -o $(TOOLS_BIN) ./cmd/tools

# 运行单节点服务器
.PHONY: run-single
run-single: build-server
	@echo "启动单节点CyberDB服务器..."
	@$(SERVER_BIN) --config=$(SINGLE_NODE_CONFIG)

# 运行主节点
.PHONY: run-master
run-master: build-server
	@echo "启动主节点CyberDB服务器..."
	@$(SERVER_BIN) --config=$(MASTER_CONFIG)

# 运行从节点
.PHONY: run-slave
run-slave: build-server
	@echo "启动从节点CyberDB服务器..."
	@$(SERVER_BIN) --config=$(SLAVE_CONFIG)

# 运行HTAP模式
.PHONY: run-htap
run-htap: build-server
	@echo "启动HTAP模式CyberDB服务器..."
	@$(SERVER_BIN) --config=$(HTAP_CONFIG)

# 创建HTAP数据目录
.PHONY: create-htap-dirs
create-htap-dirs:
	@echo "创建HTAP数据目录..."
	@mkdir -p data/htap

# 初始化HTAP环境
.PHONY: init-htap
init-htap: create-htap-dirs

# 运行HTAP集群(需要使用多个终端)
.PHONY: run-htap-cluster
run-htap-cluster:
	@echo "请在不同终端中运行以下命令:"
	@echo "  make run-htap         # 启动HTAP节点"
	@echo "  make run-client       # 启动客户端"

# 备份数据
.PHONY: backup
backup: build-tools
	@echo "备份数据库..."
	@$(TOOLS_BIN) backup --output=./backup

# 恢复数据
.PHONY: restore
restore: build-tools
	@echo "恢复数据库..."
	@$(TOOLS_BIN) restore --input=./backup/backup_latest

# 监控性能
.PHONY: monitor
monitor: build-tools
	@echo "监控数据库性能..."
	@$(TOOLS_BIN) monitor --engine=hybrid --interval=5

# 短时间监控性能
.PHONY: quick-monitor
quick-monitor: build-tools
	@echo "短时间监控数据库性能(60秒)..."
	@$(TOOLS_BIN) monitor --engine=hybrid --interval=2 --time=60

# 运行集群(需要使用多个终端)
.PHONY: run-cluster
run-cluster:
	@echo "请在不同终端中运行以下命令:"
	@echo "  make run-master   # 启动主节点"
	@echo "  make run-slave    # 启动从节点"

# 运行客户端
.PHONY: run-client
run-client: build-client
	@echo "启动CyberDB客户端..."
	@$(CLIENT_BIN)

# 测试
.PHONY: test
test:
	@echo "运行测试..."
	@$(GO) test -v ./...

# 运行HTAP性能测试
.PHONY: bench-htap
bench-htap: build-server
	@echo "运行HTAP性能测试..."
	@$(GO) test -bench=HTAP -benchtime=10s ./pkg/storage/...

# 运行存储引擎性能测试
.PHONY: bench-storage
bench-storage: build-server
	@echo "运行存储引擎性能测试..."
	@$(GO) test -bench=. -benchtime=5s ./pkg/storage/engine_bench_test.go

# 列式存储性能测试
.PHONY: bench-column
bench-column: build-server
	@echo "运行列式存储性能测试..."
	@$(GO) test -bench=Column -benchtime=5s ./pkg/storage/...

# 帮助
.PHONY: help
help:
	@echo "CyberDB Makefile帮助"
	@echo ""
	@echo "可用命令:"
	@echo "  make all           构建所有组件"
	@echo "  make build         构建服务器和客户端"
	@echo "  make build-server  仅构建服务器"
	@echo "  make build-client  仅构建客户端"
	@echo "  make build-tools   仅构建工具集"
	@echo "  make run-single    运行单节点服务器"
	@echo "  make run-master    运行主节点"
	@echo "  make run-slave     运行从节点"
	@echo "  make run-htap      运行HTAP模式服务器"
	@echo "  make init-htap     初始化HTAP环境"
	@echo "  make run-cluster   打印集群运行指令"
	@echo "  make run-htap-cluster 打印HTAP集群运行指令"
	@echo "  make run-client    运行客户端"
	@echo "  make backup        备份数据库"
	@echo "  make restore       恢复数据库"
	@echo "  make monitor       监控数据库性能"
	@echo "  make quick-monitor 短时间监控数据库性能(60秒)"
	@echo "  make bench-htap    运行HTAP性能测试"
	@echo "  make bench-storage 运行存储引擎性能测试"
	@echo "  make bench-column  运行列式存储性能测试"
	@echo "  make clean         清理构建目录"
	@echo "  make fmt           格式化代码"
	@echo "  make test          运行测试"
	@echo "  make help          显示此帮助信息" 