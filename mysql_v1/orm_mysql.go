package mysql_v1

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strconv"
	"strings"
)

// 结构体1：msyql操作结构
type ormMysql struct {
	o          *sql.DB // 数据库句柄
	dbInstance string  // 名称:"dbInstance:||" + dbCfgName + ":" + dbName
	dbCfgName  string  // 名称:default等
	dbName     string  // 数据库名称
	initErr    bool    // 初始化成功标记 0:未成功，1:成功
}

// 结构体2：字段信息结构体
type UTbDesc struct {
	Field  string // 字段名
	IsPri  bool   // 是否为主键
	Type   string // 字段类型
	Length int    // 字段长度
}

// 结构体3：批量快速读取配置参数
type UFastQuery struct {
	Table          string      // 表名
	Fields         string      // 检索的字段
	PriField       string      // 主键字段名
	PriSort        string      // 顺序 asc/desc
	RowLimit       int         // 单词取出行数
	BeginVal       interface{} // 起点(检索时不包括这条)
	BeginValIgnore bool        // 是否包含起点
}

// 数据操作1： 写入数据
// 示例:
// 	err := Insert("user" , map[string]interface{}{ "user_id":123,"user_name":"张三"} )
func (Me ormMysql) Insert(table string, row map[string]interface{}) (int64, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return 0, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	KeySql, KeyValues := Me.UtilInsert(table, row)

	// 2、写入后数据的自增Id：写入数据后数据库生成的
	KeyId := int64(0)
	if res, err := Me.o.Exec(KeySql, KeyValues...); err != nil {
		log.Error(err)
		return 0, err
	} else {
		KeyId, _ = res.LastInsertId()
	}
	return KeyId, nil
}

// 数据操作2： 批量写入数据
// 示例:
// 	err := InsertManyTransaction("user" , []map[string]interface{}{ {"user_id":123,"user_name":"张三"} } )
func (Me ormMysql) InsertManyTransaction(table string, rows []map[string]interface{}, ignore ...bool) error {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	if len(rows) <= 0 {
		return nil
	}

	var (
		fields []string
		Sql    string
		err    error
	)

	txO, err := Me.o.Begin()
	if err != nil {
		return err
	}

	for _, row := range rows {
		var values []interface{}
		// 拼凑sql
		if Sql == "" {
			var fs, vkeys []string
			for k, v := range row {
				fs = append(fs, "`"+k+"`")
				fields = append(fields, k)
				vkeys = append(vkeys, "?")
				values = append(values, v)
			}
			// 增加上ignore
			sqlIgnore := ""
			if len(ignore) > 0 && ignore[0] {
				sqlIgnore = " ignore"
			}
			Sql = "insert" + sqlIgnore + " into " + table + "(" + strings.Join(fs, ",") + ")" + "values (" + strings.Join(vkeys, ",") + ")"

		} else {
			for _, k := range fields {
				values = append(values, row[k])
			}
		}
		// 执行sql
		_, err := txO.Exec(Sql, values...)
		if err != nil {
			log.Error(err)
			_ = txO.Rollback()
			return err
		}
	}
	err = txO.Commit()
	if err != nil {
		log.Error(err)
		_ = txO.Rollback()
		return err
	}

	return nil
}

// 数据操作3： 修改数据
// 示例:
//    err := Update("user" , map[string]interface{}{ "user_id":123,"user_name":"张三"} )
func (Me ormMysql) Update(mixTable string, row map[string]interface{}, conditions map[string]interface{}) error {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	KeySql, KeyValues := Me.UtilUpdate(mixTable, row, conditions)

	// 3、执行
	if _, err := Me.o.Exec(KeySql, KeyValues...); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// 数据操作4： 替换数据
// 示例:
//    err := Replace("user" , map[string]interface{}{ "user_id":123,"user_name":"张三"} )
func (Me ormMysql) Replace(mixTable string, row map[string]interface{}) error {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	KeySql, KeyValues := Me.UtilReplace(mixTable, row)

	// 3、执行
	if _, err := Me.o.Exec(KeySql, KeyValues...); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// 数据操作5： 删除数据
// 示例:
//    err := Delete("user" , map[string]interface{}{ "user_id":123} )
func (Me ormMysql) Delete(mixTable string, conditions map[string]interface{}) error {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	KeySql, KeyValues := Me.UtilDelete(mixTable, conditions)

	// 执行sql
	if _, err := Me.o.Exec(KeySql, KeyValues...); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// 数据读取1： 常规读取(格式化sql)
// 示例:
//    maps := Query("select * from publish_homework_par_teacher where teacher_id=:teacher_id and class_id=:class_id" ,
//        map[string]interface{}{ "teacher_id":teacher_id , "class_id":class_id } ,
//        map[string]interface{}{ "offset":1 , "limit":10 } ,
//    )
func (Me ormMysql) Query(sql string, ConOpt ...map[string]interface{}) ([]map[string]interface{}, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 1、条件参数和限制参数处理
	KeyConditions := map[string]interface{}{}
	if len(ConOpt) > 0 {
		KeyConditions = ConOpt[0]
	}
	KeyOptions := map[string]interface{}{}
	if len(ConOpt) > 1 {
		KeyOptions = ConOpt[1]
	}

	// 2、sql整合
	KeySql, KeyArgs := utilMakeCondition(sql, KeyConditions)
	if _, ok := KeyOptions["limit"]; ok {
		if _, ok := KeyOptions["offset"]; ok {
			KeySql += " limit ?,?"
			KeyArgs = append(KeyArgs, KeyOptions["offset"], KeyOptions["limit"])
		} else {
			KeySql += " limit ?"
			KeyArgs = append(KeyArgs, KeyOptions["limit"])
		}
	}

	// 3、读取的数据：从数据表里读取
	KeyRows := make([]map[string]interface{}, 0)
	if List, err := Me.o.Query(KeySql, KeyArgs...); err != nil {
		log.Error(err)
		return nil, err
	} else {
		// cols, _ := rows.Columns()
		KeyRows, err = utilScan(List)
		_ = List.Close()
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}

	return KeyRows, nil
}

// 数据读取2： 常规读取(直接执行参数sql和参数)
// 示例:
//	data,err:=QueryRow("select * from demo where id=?",[]interface{}{123})
func (Me ormMysql) QueryRaw(qSql string, qArgs ...interface{}) ([]map[string]interface{}, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	List, err := Me.o.Query(qSql, qArgs...)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	rows, err := utilScan(List)
	_ = List.Close()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rows, nil
}

// 数据读取3： 指定数据表读取一批数据
// 示例:
//    Data,err := QueryTable("publish_homework_par_teacher" , "*",
//        map[string]interface{}{ "teacher_id":teacher_id , "class_id":class_id } ,
//        map[string]interface{}{ "offset":1 , "limit":10 } ,
//    )
func (Me ormMysql) QueryTable(table string, fields string, ConOpt ...map[string]interface{}) ([]map[string]interface{}, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 默认参数
	var (
		conditions = map[string]interface{}{}
		options    = map[string]interface{}{}
	)
	if len(ConOpt) > 0 {
		conditions = ConOpt[0]
	}
	if len(ConOpt) > 1 {
		options = ConOpt[1]
	}
	// 变量定义
	var (
		err error
	)

	Sql := "select " + fields + " from " + table + " where 1 "

	//
	for k := range conditions {
		Sql += " and " + k + "=:" + k
	}

	qSql, qArgs := utilMakeCondition(Sql, conditions)

	// 拼凑limit
	if _, ok := options["limit"]; ok {
		if _, ok := options["offset"]; ok {
			qSql += " limit ?,?"
			qArgs = append(qArgs, options["offset"], options["limit"])
		} else {
			qSql += " limit ?"
			qArgs = append(qArgs, options["limit"])
		}
	}

	List, err := Me.o.Query(qSql, qArgs...)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	rows, err := utilScan(List)
	_ = List.Close()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rows, nil
}

// 数据读取4： 指定数据表读取一条数据
// 示例:
//    Row,err := QueryTableOne("publish_homework_par_teacher" , "*",
//        map[string]interface{}{ "teacher_id":teacher_id , "class_id":class_id } ,
//    )
// 说明：未找到，返回的数据体为nil
func (Me ormMysql) QueryTableOne(table string, fields string, Condition ...map[string]interface{}) (map[string]interface{}, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 1、检索属性
	KeyOption := map[string]interface{}{"limit": 1}

	// 2、检索条件，读取Condition第一个值作为检索条件
	KeyConditions := map[string]interface{}{}
	if len(Condition) > 1 {
		return nil, errors.New("只需要2个参数")
	} else if len(Condition) == 1 {
		KeyConditions = Condition[0]
	}

	// 3、查询数据
	KeyRetData := map[string]interface{}{}
	if data, err := Me.QueryTable(table, fields, KeyConditions, KeyOption); err != nil {
		return KeyRetData, err
	} else {
		// 有数据
		if len(data) > 0 {
			KeyRetData = data[0]
		}
	}

	return KeyRetData, nil
}

// 表结构信息1：获取实例里全部数据库
func (Me ormMysql) NameAllDbs(dbIgnores ...string) ([]string, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 1、忽略的数据库：默认的加上参数
	KeyIgnoreDbs := map[string]interface{}{
		"mysql":              "",
		"information_schema": "",
		"test":               "",
	}
	for _, v := range dbIgnores {
		KeyIgnoreDbs[v] = ""
	}

	// 2、返回变量: 从数据库里获取，过滤掉 KeyIgnoreDbs 的数据库名
	KeyRet := make([]string, 0)
	if res, err := Me.Query("show databases"); err != nil {
		return []string{}, err
	} else {
		// 2.2、构造返回数据
		for _, v := range res {
			db := v["Database"].(string)
			if _, ok := KeyIgnoreDbs[db]; !ok {
				KeyRet = append(KeyRet, db)
			}
		}
	}

	// 4、返回
	return KeyRet, nil
}

// 表结构信息2：获取实例里指定数据库名的数据表
func (Me ormMysql) NameAllTablesOneDb() ([]string, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 1、返回变量：数据库获取
	var KeyRet []string
	if res, err := Me.Query("show tables"); err != nil {
		return []string{}, err
	} else {
		// 3、构造返回数据
		for _, v := range res {
			tb := v["Tables_in_"+Me.dbName].(string)
			KeyRet = append(KeyRet, tb)
		}
	}

	// 4、返回
	return KeyRet, nil
}

// 表结构信息3：获取数据表创建语句
func (Me ormMysql) ShowCreateTable(table string) (string, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return "", errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 1、变量定义
	KeySqlRet := ""
	// 2、读取数据库
	if List, err := Me.o.Query("show create table `" + table + "`"); err != nil {
		log.Error(err)
		return "", err
	} else {
		rows, err := utilScan(List)
		_ = List.Close()
		if err != nil {
			log.Error(err)
			return "", err
		}
		KeySqlRet = rows[0]["Create Table"].(string)
	}

	// 3、返回数据
	return KeySqlRet, nil
}

// 表结构信息4：获取数据表字段信息
func (Me ormMysql) DescTable(tbName string) (map[string]UTbDesc, error) {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return nil, errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 1、返回变量
	KeyTbDesc := map[string]UTbDesc{}

	// 2、读取数据
	if res, err := Me.Query("desc " + tbName); err != nil {
		log.Error(err)
		return nil, err
	} else {
		// 3、构造返回数据
		for _, v := range res {
			o := UTbDesc{Field: v["Field"].(string), IsPri: false}
			d := strings.Split(v["Type"].(string), "(")
			o.Type = d[0]
			if len(d) > 1 {
				d1 := strings.Split(d[1], ")")
				o.Length, _ = strconv.Atoi(d1[0])
			}
			if v["Key"] == "PRI" {
				o.IsPri = true
			}
			KeyTbDesc[v["Field"].(string)] = o
		}
	}

	// 4、返回
	return KeyTbDesc, nil
}

// 特殊1：直接执行sql
// 示例:
//    err := Exec("alter table user rename user_old" )
func (Me ormMysql) Exec(Sql string) error {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	// 执行sql
	if _, err := Me.o.Exec(Sql); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// 特殊2：mysql中获取全表数据
// 	err := QueryAllCircle(mysql_v1.UFastQuery{
// 		Table:           "tbl_resource_main",
// 		Fields:          "*",
// 		PriField:        "id",
// 		PriSort:         "asc",
// 		RowLimit:        2000,
// 		// BeginVal:        3,
// 		// beginValIgnore: true,
// 	},func(data map[string]interface{}) bool{
// 		fmt.Println(len(data))
// 		return true	// true:继续 false：终止
// 	})
func (Me ormMysql) QueryAllCircle(Cfg UFastQuery, backFunc func(V map[string]interface{}) bool) error {
	if Me.initErr {
		log.Error("数据库未连接成功", Me.dbCfgName, Me.dbName)
		return errors.New("数据库未连接成功:" + Me.dbCfgName + " . " + Me.dbName)
	}

	var (
		table          = Cfg.Table          // 表名
		fields         = Cfg.Fields         // 检索的字段
		priField       = Cfg.PriField       // 主键字段名
		priSort        = Cfg.PriSort        // 顺序 asc/desc
		rowLimit       = Cfg.RowLimit       // 单词取出行数
		beginVal       = Cfg.BeginVal       // 最后一次取出标记，
		beginValIgnore = Cfg.BeginValIgnore // 默认包含beginVal起点数据，设置为true，则忽略beginVal起点数据
		compare        = ">"
		Sql            = ""
		groupNum       = 0
		// priFieldIsString = true
	)

	// 获取表结构，识别主键类型
	dbDesc, err := Me.DescTable(Cfg.Table)
	if err != nil {
		log.Error(err)
		return err
	}
	if _, ok := dbDesc[Cfg.PriField]; !ok {
		return errors.New(Cfg.PriField + " 不是主键")
	}
	if !dbDesc[Cfg.PriField].IsPri {
		return errors.New(Cfg.PriField + " 不是主键")
	}
	// if dbDesc[Cfg.PriField].Type == "int" || dbDesc[Cfg.PriField].Type == "bigint" {
	// 	priFieldIsString = false
	// }

	// 取出起点
	if beginVal == nil {
		res, err := Me.Query("select "+priField+" from "+table+" order by "+priField+" "+priSort,
			map[string]interface{}{}, map[string]interface{}{
				"limit": 1,
			},
		)
		if err != nil {
			log.Error(err)
			return err
		}
		// 没有数据
		if len(res) == 0 {
			return nil
		}
		beginVal = res[0][priField]
	}

	// 1、读取数据
	if priSort == "desc" {
		compare = "<"
	}
	for {
		// 1.1、拼凑mysql
		if groupNum == 0 && !beginValIgnore {
			if priSort == "desc" {
				compare = "<="
			} else {
				compare = ">="
			}
		} else if groupNum == 1 {
			if priSort == "desc" {
				compare = "<"
			} else {
				compare = ">"
			}
		}
		if groupNum <= 1 {
			Sql = `
		        select ` + fields + `,` + priField + ` from ` + table + `
		        where ` + priField + compare + `:` + priField + `
		        order by ` + priField + ` ` + priSort
		}
		// 1.2、读取数据
		maps, err := Me.Query(Sql,
			map[string]interface{}{
				priField: beginVal,
			}, map[string]interface{}{
				"limit": rowLimit,
			},
		)
		if err != nil {
			log.Error(err)
			return err
		}

		// 1.3、刷新 mysqlTablePriIdVal
		rowNum := len(maps)
		if rowNum > 0 {
			beginVal = maps[len(maps)-1][priField]
		}
		// 1.4、数据回调,返回为false则停止读取
		_continue := true
		for _, v := range maps {
			if !backFunc(v) {
				_continue = false
				break
			}
		}
		if !_continue {
			return nil
		}
		groupNum++

		if rowNum < rowLimit {
			break
		}
	}
	return nil
}

// 获取insert的sql和参数
func (Me ormMysql) UtilInsert(table string, row map[string]interface{}) (string, []interface{}) {
	// 1、条件参数：从参数里拼凑
	KeyFields := make([]string, 0)
	KeyFieldFlag := make([]string, 0)
	KeyValues := make([]interface{}, 0)
	for k, v := range row {
		KeyFields = append(KeyFields, "`"+k+"`")
		KeyFieldFlag = append(KeyFieldFlag, "?")
		KeyValues = append(KeyValues, v)
	}

	// 2、写入后数据的自增Id：写入数据后数据库生成的
	KeySql := `
		insert into ` + table + `(` + strings.Join(KeyFields, ",") + `)
		values (` + strings.Join(KeyFieldFlag, ",") + `)`
	return KeySql, KeyValues
}

// 获取replace的sql和参数
func (Me ormMysql) UtilReplace(mixTable string, row map[string]interface{}) (string, []interface{}) {
	// 1、条件参数：从参数里拼凑
	KeyFields := make([]string, 0)
	KeyFieldFlag := make([]string, 0)
	KeyValues := make([]interface{}, 0)
	for k, v := range row {
		KeyFields = append(KeyFields, "`"+k+"`")
		KeyFieldFlag = append(KeyFieldFlag, "?")
		KeyValues = append(KeyValues, v)
	}

	// 2、数据表名处理
	KeyTable := "`" + mixTable + "`"
	if strings.Contains(mixTable, ".") {
		KeyTable = mixTable
	}

	// 3、拼凑sql
	KeySql := `
		replace into ` + KeyTable + `(` + strings.Join(KeyFields, ",") + `)
		values (` + strings.Join(KeyFieldFlag, ",") + `)`
	return KeySql, KeyValues
}

// 获取update的sql和参数
func (Me ormMysql) UtilUpdate(mixTable string, row map[string]interface{}, conditions map[string]interface{}) (string, []interface{}) {
	// 1、参数拼凑
	KeyConditionFields := make([]string, 0)
	KeyUpdateFields := make([]string, 0)
	KeyValues := make([]interface{}, 0)
	for k, v := range row {
		KeyUpdateFields = append(KeyUpdateFields, "`"+k+"`=?")
		KeyValues = append(KeyValues, v)
	}
	for k, v := range conditions {
		KeyConditionFields = append(KeyConditionFields, "`"+k+"`=?")
		KeyValues = append(KeyValues, v)
	}

	// 2、数据表名处理
	KeyTable := "`" + mixTable + "`"
	if strings.Contains(mixTable, ".") {
		KeyTable = mixTable
	}

	// 3、执行
	KeySql := `
			update ` + KeyTable + `
			set ` + strings.Join(KeyUpdateFields, ",") + `
			where ` + strings.Join(KeyConditionFields, " and ") + `
		`
	return KeySql, KeyValues
}

// 获取delete的sql和参数
func (Me ormMysql) UtilDelete(mixTable string, conditions map[string]interface{}) (string, []interface{}) {

	// 拼凑sql
	var (
		fields []string
		values []interface{}
	)
	for k, v := range conditions {
		fields = append(fields, "`"+k+"`=?")
		values = append(values, v)
	}
	table := "`" + mixTable + "`"
	if strings.Contains(mixTable, ".") {
		table = mixTable
	}
	Sql := "delete from " + table + " where " + strings.Join(fields, " and ")

	return Sql, values
}

// 辅助函数1: sql条件拼凑处理
// 示例：
//    Sql,Args := utilMakeCondition("select * from publish_homework_par_teacher where teacher_id=:teacher_id and class_id=:class_id" ,
//        map[string]interface{}{ "teacher_id":teacher_id , "class_id":class_id }
//    )
func utilMakeCondition(sql string, conditions map[string]interface{}) (string, []interface{}) {
	// 查找连续的单词字母
	var retArgs []interface{}
	retSql := regexp.MustCompile(`:[\w]+`).ReplaceAllStringFunc(sql, func(s string) string {
		retArgs = append(retArgs, conditions[s[1:]])
		return "?"
	})

	return retSql, retArgs
}

// 辅助函数2: mysql查询结果数据转换成map数组数据
func utilScan(List *sql.Rows) ([]map[string]interface{}, error) {
	fields, _ := List.Columns()
	rows := make([]map[string]interface{}, 0)

	// 遍历数据
	for List.Next() {
		// 内容数据scans：从list中提取
		scans := make([]interface{}, len(fields))
		for i := range scans {
			scans[i] = &scans[i]
		}
		err := List.Scan(scans...)
		if err != nil {
			return nil, err
		}

		// 一行数据row：从scans里提取
		row := make(map[string]interface{})
		for i, v := range scans {
			var value interface{}
			switch inst := v.(type) {
			case nil:
				value = nil
			case int64:
				value = strconv.FormatInt(inst, 10)
			case int:
				value = strconv.Itoa(inst)
			case []byte:
				value = string(inst)
			default:
				value = v
				log.Panic(fields[i], "default")
			}
			row[fields[i]] = value
		}
		rows = append(rows, row)
	}
	return rows, nil
}
