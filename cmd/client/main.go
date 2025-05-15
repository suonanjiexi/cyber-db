package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	host     string
	port     int
	user     string
	password string
	database string
)

func init() {
	flag.StringVar(&host, "host", "localhost", "CyberDB服务器地址")
	flag.IntVar(&port, "port", 3306, "CyberDB服务器端口")
	flag.StringVar(&user, "user", "root", "用户名")
	flag.StringVar(&password, "password", "", "密码")
	flag.StringVar(&database, "database", "", "数据库名")
}

func main() {
	flag.Parse()

	// 连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		user, password, host, port, database)

	// 连接数据库
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("连接CyberDB失败: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 设置连接参数
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// 测试连接
	if err := db.Ping(); err != nil {
		fmt.Printf("CyberDB连接测试失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("连接CyberDB成功!")
	fmt.Printf("CyberDB客户端 (服务器: %s:%d)\n", host, port)
	fmt.Println("输入SQL语句或命令 (输入'exit'退出, 'help'获取帮助):")

	// 创建输入扫描器
	scanner := bufio.NewScanner(os.Stdin)
	var sqlBuffer strings.Builder

	// 命令行循环
	for {
		fmt.Print("cyberdb> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		trimmedInput := strings.TrimSpace(input)

		// 处理特殊命令
		if trimmedInput == "" {
			continue
		} else if trimmedInput == "exit" || trimmedInput == "quit" {
			fmt.Println("再见!")
			break
		} else if trimmedInput == "help" {
			printHelp()
			continue
		} else if strings.HasPrefix(trimmedInput, "use ") {
			// 切换数据库
			newDB := strings.TrimSpace(trimmedInput[4:])
			if newDB != "" {
				database = newDB
				// 重新连接
				db.Close()
				dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
					user, password, host, port, database)
				db, err = sql.Open("mysql", dsn)
				if err != nil {
					fmt.Printf("切换数据库失败: %v\n", err)
					continue
				}
				fmt.Printf("数据库已切换到: %s\n", database)
			}
			continue
		}

		// 添加输入到SQL缓冲区
		sqlBuffer.WriteString(input)

		// 检查SQL语句是否完整(以分号结尾)
		if strings.HasSuffix(trimmedInput, ";") {
			// 执行SQL语句
			sql := sqlBuffer.String()
			sqlBuffer.Reset()

			// 移除末尾的分号
			sql = strings.TrimSuffix(sql, ";")

			// 检查SQL类型
			upperSQL := strings.ToUpper(strings.TrimSpace(sql))
			if strings.HasPrefix(upperSQL, "SELECT") {
				// 查询操作
				executeQuery(db, sql)
			} else {
				// 非查询操作
				executeNonQuery(db, sql)
			}
		} else {
			// SQL语句不完整，继续添加输入
			sqlBuffer.WriteString(" ")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("读取输入错误: %v\n", err)
	}
}

// printHelp 打印帮助信息
func printHelp() {
	fmt.Println("CyberDB客户端命令:")
	fmt.Println("  exit, quit - 退出客户端")
	fmt.Println("  help       - 显示此帮助信息")
	fmt.Println("  use <db>   - 切换数据库")
	fmt.Println("")
	fmt.Println("SQL示例:")
	fmt.Println("  CREATE DATABASE test;")
	fmt.Println("  USE test;")
	fmt.Println("  CREATE TABLE users (id INT, name VARCHAR(255), age INT);")
	fmt.Println("  INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);")
	fmt.Println("  SELECT * FROM users;")
}

// executeQuery 执行查询操作
func executeQuery(db *sql.DB, query string) {
	// 执行查询
	rows, err := db.Query(query)
	if err != nil {
		fmt.Printf("查询错误: %v\n", err)
		return
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("获取列信息错误: %v\n", err)
		return
	}

	// 准备结果容器
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 打印列名
	for i, col := range columns {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(col)
	}
	fmt.Println()

	// 打印分隔线
	for i := range columns {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print("--------")
	}
	fmt.Println()

	// 打印结果行
	rowCount := 0
	for rows.Next() {
		// 扫描行数据
		err := rows.Scan(valuePtrs...)
		if err != nil {
			fmt.Printf("扫描行数据错误: %v\n", err)
			return
		}

		// 打印行数据
		for i, v := range values {
			if i > 0 {
				fmt.Print("\t")
			}
			if v == nil {
				fmt.Print("NULL")
			} else {
				fmt.Print(v)
			}
		}
		fmt.Println()
		rowCount++
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("遍历结果集错误: %v\n", err)
		return
	}

	fmt.Printf("%d 行记录\n", rowCount)
}

// executeNonQuery 执行非查询操作
func executeNonQuery(db *sql.DB, sql string) {
	result, err := db.Exec(sql)
	if err != nil {
		fmt.Printf("执行错误: %v\n", err)
		return
	}

	// 获取受影响的行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		fmt.Printf("获取受影响行数错误: %v\n", err)
		return
	}

	// 获取最后插入的ID
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		// 忽略错误，有些操作没有最后插入ID
		lastInsertID = 0
	}

	// 根据SQL类型输出不同信息
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upperSQL, "INSERT") {
		fmt.Printf("插入成功，受影响行数: %d，最后插入ID: %d\n", rowsAffected, lastInsertID)
	} else if strings.HasPrefix(upperSQL, "UPDATE") {
		fmt.Printf("更新成功，受影响行数: %d\n", rowsAffected)
	} else if strings.HasPrefix(upperSQL, "DELETE") {
		fmt.Printf("删除成功，受影响行数: %d\n", rowsAffected)
	} else if strings.HasPrefix(upperSQL, "CREATE DATABASE") {
		fmt.Println("数据库创建成功")
	} else if strings.HasPrefix(upperSQL, "CREATE TABLE") {
		fmt.Println("表创建成功")
	} else if strings.HasPrefix(upperSQL, "DROP DATABASE") {
		fmt.Println("数据库删除成功")
	} else if strings.HasPrefix(upperSQL, "DROP TABLE") {
		fmt.Println("表删除成功")
	} else {
		fmt.Printf("操作成功，受影响行数: %d\n", rowsAffected)
	}
}
