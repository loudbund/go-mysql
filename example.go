package main

import (
	"fmt"
	"github.com/loudbund/go-mysql/mysql_v1"
	"log"
	"time"
)

func init() {
	mysql_v1.Init("test.conf")
}

// 1、全表快速读取
func QueryAllCircle() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("Start QueryAllCircle")

	// 1、读取数据
	Len := 0
	err := mysql_v1.Handle().QueryAllCircle(mysql_v1.UFastQuery{
		Table:          "demo",
		Fields:         "*",
		PriField:       "id",
		PriSort:        "asc",
		RowLimit:       2000,
		BeginVal:       nil,
		BeginValIgnore: false,
	}, func(V map[string]interface{}) bool {
		Len++
		return true
	})
	if err != nil {
		log.Panic(err)
	}

	fmt.Println(Len)
}

// 3、写入数据
func Insert() int64 {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start Insert ============")

	id, err := mysql_v1.Handle().Insert("demo", map[string]interface{}{
		"status":  1,
		"debug":   "test Insert",
		"creator": "123",
	})
	fmt.Println(id, err)

	return id
}

// 3、写入数据
func InsertIgnore() int64 {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start InsertIgnore ============")

	id, err := mysql_v1.Handle().Insert("demo", map[string]interface{}{
		"id":      1,
		"status":  1,
		"debug":   "test Insert",
		"creator": "123",
	}, true)
	fmt.Println(id, err)

	return id
}

// 4、写入多条
func InsertMany() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start InsertMany ============")

	err := mysql_v1.Handle().InsertManyTransaction("demo", []map[string]interface{}{
		{
			"status":  1,
			"debug":   "test InsertMany",
			"creator": "123",
		},
	})
	fmt.Println(err)
}

// 2、数据查询
func Update(Id int64) {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start Update ============")

	// 1、读取数据
	err := mysql_v1.Handle().Update("demo", map[string]interface{}{
		"status":  2,
		"creator": "test Update",
	}, map[string]interface{}{
		"id": Id,
	})
	if err != nil {
		log.Panic(err)
	}
}

// 2、数据查询
func Replace(Id int64) {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start Replace ============")

	// 1、读取数据
	err := mysql_v1.Handle().Replace("demo", map[string]interface{}{
		"status":  2,
		"creator": "Replace ",
		"id":      Id,
	})
	if err != nil {
		log.Panic(err)
	}
}

// 2、数据查询
func Query(Id interface{}) {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start Query ============")

	// 1、读取数据
	data, err := mysql_v1.Handle().Query("select * from demo where id=:id", map[string]interface{}{
		"id": Id,
	})
	if err != nil {
		log.Panic(err)
	}

	// 2、打印结果
	for k, v := range data {
		for m, n := range v {
			fmt.Println(k, m, n)
		}
	}
}

// 2、数据查询
func Delete(Id int64) {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start Delete ============")

	// 1、读取数据
	err := mysql_v1.Handle().Delete("demo", map[string]interface{}{
		"id": Id,
	})
	if err != nil {
		log.Panic(err)
	}
}

func QueryRaw() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start QueryRaw ============")

	data, err := mysql_v1.Handle().QueryRaw("select * from demo limit 1")
	if err != nil {
		log.Panic(err)
	}

	// 2、打印结果
	for k, v := range data {
		for m, n := range v {
			fmt.Println(k, m, n)
		}
	}
}

func QueryTable() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start QueryTable ============")

	data, err := mysql_v1.Handle().QueryTable("demo", "*")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(len(data))
}

func QueryTableOne() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start QueryTableOne ============")

	data, err := mysql_v1.Handle().QueryTableOne("demo", "*")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(len(data))
}

func ShowCreateTable() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start ShowCreateTable ============")

	Sql, err := mysql_v1.Handle().ShowCreateTable("demo")
	if err != nil {
		log.Panic(err)
	}
	fmt.Println(Sql)
}

func DescTable() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start DescTable ============")

	Data, err := mysql_v1.Handle().DescTable("demo")
	if err != nil {
		log.Panic(err)
	}
	for k, v := range Data {
		fmt.Println(k, v)
	}
}
func NameAllDbs() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start NameAllDbs ============")

	Data, err := mysql_v1.Handle().NameAllDbs()
	if err != nil {
		log.Panic(err)
	}
	for k, v := range Data {
		fmt.Println(k, v)
	}
}
func NameAllTablesOneDb() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start NameAllTablesOneDb ============")

	Data, err := mysql_v1.Handle("default", "mysql").NameAllTablesOneDb()
	if err != nil {
		log.Panic(err)
	}
	for k, v := range Data {
		fmt.Println(k, v)
	}
}

func Exec() {
	defer func(T time.Time) { fmt.Println(time.Since(T).String()) }(time.Now())
	fmt.Println("========= Start Exec ============")

	err := mysql_v1.Handle().Exec(`
		CREATE TABLE demo (
			id int(11) NOT NULL AUTO_INCREMENT,
			status tinyint(4) DEFAULT NULL,
			debug varchar(255) NOT NULL DEFAULT '' COMMENT '数据变更说明',
		creator varchar(20) NOT NULL DEFAULT '' COMMENT '创建者',
		created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COMMENT='测试数据表'
	`)
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	Exec()

	IdA := InsertIgnore()
	fmt.Println("InsertIgnore 返回id：", IdA)

	// 数据调整操作
	Id := Insert()
	InsertMany() // 此处写入的数据id为 Id+1
	Update(Id)
	Id1 := Insert()
	Replace(Id1)
	Delete(Id + 1)

	// 数据检索
	Query(Id + 1)
	QueryRaw()
	QueryTable()
	QueryTableOne()

	// 库表信息获取
	NameAllDbs()
	NameAllTablesOneDb()
	ShowCreateTable()
	DescTable()

	// 快速批量获取
	QueryAllCircle()
}
