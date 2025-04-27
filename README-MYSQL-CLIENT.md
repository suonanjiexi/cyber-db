# 使用MySQL客户端连接CyberDB

本文档介绍如何使用Navicat Premium等MySQL客户端工具连接CyberDB服务器。

## 启动服务器

双击 `start-server.bat` 脚本启动CyberDB服务器。服务器将在本机的3306端口上监听连接。

## 使用Navicat连接

1. 打开Navicat Premium
2. 点击"连接" -> "MySQL"
3. 填写连接信息：
   - 连接名称：CyberDB (任意名称)
   - 主机：localhost (如果是远程连接，请使用服务器IP地址)
   - 端口：3306
   - 用户名：root (任意用户名都可以，目前版本不验证用户名)
   - 密码：(留空，当前版本不验证密码)
4. 点击"确定"保存连接
5. 双击创建的连接，连接到CyberDB服务器

## 支持的SQL语法

CyberDB支持以下基本SQL命令：

### 创建表
```sql
CREATE TABLE users (id, name, email, age)
```

### 插入数据
```sql
INSERT INTO users (id, name, email, age) VALUES (1, "张三", "zhangsan@example.com", "30")
```

### 查询数据
```sql
SELECT * FROM users
SELECT name, email FROM users WHERE age > 25
SELECT * FROM users ORDER BY name
```

### 更新数据
```sql
UPDATE users SET age = 31 WHERE id = 1
```

### 删除数据
```sql
DELETE FROM users WHERE id = 1
```

### 创建索引
```sql
CREATE INDEX idx_user_name ON users (name)
CREATE UNIQUE INDEX idx_user_email ON users (email)
```

## 常见问题

1. **连接被拒绝**：确保服务器正在运行，并且防火墙没有阻止3306端口。

2. **权限错误**：当前版本不验证用户名和密码，所以这不应该是问题。

3. **SQL语法错误**：CyberDB支持的SQL语法是MySQL兼容的子集，某些高级特性可能不支持。

## 注意事项

- CyberDB服务器默认将数据存储在`./data`目录中
- 每次服务器启动时，它将加载上次会话中保存的数据
- 要备份数据，只需复制`./data`目录即可 