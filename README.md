# go-mysql
go-pool是基于database/sql和github.com/go-sql-driver/mysql提供的一组数据库快速操作的函数库，使用前需要作些准备
1. 需要数据库的配置文件
2. 初始化配置
3. 获取数据库句柄
4. 调用函数操作数据表

## 安装
go get github.com/loudbund/go-mysql

## 配置文件示例
```db.conf
# 默认数据库
[db_default]
host        = 127.0.0.1     ; 数据库ip
port        = 3306          ; 数据库端口
db          = test          ; 数据库名
username    = root          ; 连接账号
password    = root123456    ; 连接密码
charset     = utf8          ; 编码: utf8/utf8mb4
maxIdle     = 7             ; 空闲连接数
maxConn     = 19            ; 最大连接数

# 指定数据库
[db_test]
host        = 182.10.0.102
port        = 3306
db          = test
username    = root
password    = root123456
charset     = utf8       # utf8/utf8mb4
maxIdle     = 7
maxConn     = 19
```

## 指定配置文件和初始化
1. 可以直接在main.go的init里初始
2. 初始化的时候，所有配置了的数据库都会连接检测，连不上就抛出panic
```golang
func init() {
	mysql_v1.Init("test.conf")
}
```

## 获取数据库句柄
1. 使用默认配置 mysql_v1.Handle() , 将读取 [db_default] 段配置
2. 指定数据库配置  mysql_v1.Handle("test") , 将读取 [db_test] 段配置
3. 指定数据库配置，指定数据库  mysql_v1.Handle("test","user") , 将读取 [db_test] 段配置, **数据库名换成user库**
```golang
handle := mysql_v1.Handle()
handle1 := mysql_v1.Handle("test")
handle2 := mysql_v1.Handle("test", "user")
```

## 数据库常规操作-表内容调整 函数
```golang
mysql_v1.Handle().Insert
mysql_v1.Handle().InsertManyTransaction
mysql_v1.Handle().Update
mysql_v1.Handle().Replace
mysql_v1.Handle().Delete
```

## 数据库常规操作-数据检索 函数
```golang
mysql_v1.Handle().Query
mysql_v1.Handle().QueryRaw
mysql_v1.Handle().QueryTable
mysql_v1.Handle().QueryTableOne
```

## 表信息获取 函数
```golang
mysql_v1.Handle().NameAllDbs
mysql_v1.Handle().NameAllTablesOneDb
mysql_v1.Handle().ShowCreateTable
mysql_v1.Handle().DescTable
```

## 特殊函数 函数
```golang
mysql_v1.Handle().Exec
mysql_v1.Handle().QueryAllCircle
```
## end
