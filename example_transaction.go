package main

import (
	"database/sql"
	"fmt"
	"github.com/loudbund/go-mysql/mysql_v1"
	"log"
)

func init() {
	mysql_v1.Init("test.conf")
}

func main() {
	runTransaction()
}

func runTransaction() {
	Db := mysql_v1.Handle().GetDb()
	var KeyTx *sql.Tx
	if tx, err := Db.Begin(); err != nil {
		log.Panic(err)
	} else {
		KeyTx = tx
	}

	if true {
		Sql, Vals := mysql_v1.Handle().UtilInsert("demo", map[string]interface{}{
			"status":  1,
			"debug":   "test Insert11",
			"creator": "123",
		})
		if _, err := KeyTx.Exec(Sql, Vals...); err != nil {
			fmt.Println(err)
			_ = KeyTx.Rollback()
			return
		}
	}
	if true {
		Sql, vals := mysql_v1.Handle().UtilReplace("demo", map[string]interface{}{
			"id":      2,
			"status":  1,
			"debug":   "test Insert",
			"creator": "123",
		})
		if _, err := KeyTx.Exec(Sql, vals...); err != nil {
			fmt.Println(err)
			_ = KeyTx.Rollback()
			return
		}
	}
	if true {
		Sql, vals := mysql_v1.Handle().UtilReplace("demo", map[string]interface{}{
			"id":      2,
			"status":  1,
			"debug":   "test Insert replace",
			"creator": "123",
		})
		if _, err := KeyTx.Exec(Sql, vals...); err != nil {
			fmt.Println(err)
			_ = KeyTx.Rollback()
			return
		}
	}
	if true {
		Sql, vals := mysql_v1.Handle().UtilUpdate("demo", map[string]interface{}{
			"id":      3,
			"status":  1,
			"debug":   "test Insert update",
			"creator": "123",
		}, map[string]interface{}{
			"id": 3,
		})
		if _, err := KeyTx.Exec(Sql, vals...); err != nil {
			fmt.Println(err)
			_ = KeyTx.Rollback()
			return
		}
	}

	if true {
		Sql, vals := mysql_v1.Handle().UtilReplace("demo", map[string]interface{}{
			"id":      5,
			"status":  1,
			"debug":   "test Insert replace",
			"creator": "123",
		})
		if _, err := KeyTx.Exec(Sql, vals...); err != nil {
			fmt.Println(err)
			_ = KeyTx.Rollback()
			return
		}
	}

	if true {
		Sql, vals := mysql_v1.Handle().UtilDelete("demo", map[string]interface{}{
			"id": 5,
		})
		if _, err := KeyTx.Exec(Sql, vals...); err != nil {
			fmt.Println(err)
			_ = KeyTx.Rollback()
			return
		}
	}

	if err := KeyTx.Commit(); err != nil {
		_ = KeyTx.Rollback()
	}
}
