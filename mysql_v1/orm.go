package mysql_v1

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/larspensjo/config"
	log "github.com/sirupsen/logrus"
)

// 全局变量 -------------------------------------------------------------------------
var (
	handles    = make(map[string]*ormMysql) // 实例句柄
	pathConfig = ""                         // 配置文件地址
	dbSections = map[string]string{}        // 名称=> 数据库
)

// @Title 初始化配置文件路径
func Init(cfgPath string) {

	// 配置文件路径赋值
	pathConfig = cfgPath

	// 读取配置文件
	cfg, err := config.ReadDefault(pathConfig)
	if err != nil {
		log.Panic("读取配置文件出错" + err.Error())
		return
	}

	// 初始化所有数据库配置项的连接
	for _, v := range cfg.Sections() {
		if v[:3] == "db_" {
			dbSections[v[3:]], _ = cfg.String(v, "db")
			_, err := getConnectedHandle(v[3:])
			if err != nil {
				log.Panic("数据库连接初始化失败", v)
			}
		}
	}
}

// 初始化
func getConnectedHandle(dbCfgName string, varDbName ...string) (*ormMysql, error) {
	// 判断配置文件是否已赋值
	if pathConfig == "" {
		log.Panic("请先初始化设置数据库配置文件")
		return nil, errors.New("请先初始化设置数据库配置文件")
	}

	// 没有数据库配置项
	if _, ok := dbSections[dbCfgName]; !ok {
		log.Error(pathConfig + " 没有数据库 " + dbCfgName + " 这个配置项 ")
		return nil, errors.New(pathConfig + " 没有数据库 " + dbCfgName + " 这个配置项 ")
	}

	// 数据库名称
	dbName := dbSections[dbCfgName]
	if len(varDbName) > 0 {
		dbName = varDbName[0]
	}

	// 定义实例map键值名称
	var dbInstance = "dbInstance|" + dbCfgName + "|" + dbName

	// 句柄已存在，直接返回
	if _, ok := handles[dbInstance]; ok {
		return handles[dbInstance], nil
	}

	// 读取配置文件
	host, port, db, username, password, charset, maxIdle, maxConn, err := getDbConfig("db_" + dbCfgName)
	if err != nil {
		log.Error("读取配置文件出错:" + err.Error())
		return nil, err
	}

	// 参数传了数据库名称，则使用传入的数据库名称
	if dbName != "" {
		db = dbName
	}

	// 连接数据库

	dbHandle, err := sql.Open("mysql", username+":"+password+"@tcp("+host+":"+port+")/"+db+"?charset="+charset)
	if err != nil {
		log.Error(err)
		log.Error("数据库:"+dbCfgName+" . "+dbName, " 连接失败")
		return nil, err
	}
	dbHandle.SetMaxOpenConns(maxConn)
	dbHandle.SetMaxIdleConns(maxIdle)

	handles[dbInstance] = new(ormMysql)
	handles[dbInstance].o = dbHandle
	handles[dbInstance].dbInstance = dbInstance
	handles[dbInstance].dbCfgName = dbCfgName
	handles[dbInstance].dbName = dbName

	return handles[dbInstance], nil
}

// @Title 获取数据库句柄，所有配置信息从配置文件读取
func Handle(Name ...string) *ormMysql {
	// 有参数使用传入的参数，否则使用default
	if len(Name) == 0 {
		Name = append(Name, "default")
	}

	// 获取指定配置和库名的句柄
	handle, err := getConnectedHandle(Name[0], Name[1:]...)
	if err != nil {
		return &ormMysql{initErr: true}
	}

	// 返回连接句柄
	return handle
}

// @Title 获取配置文件
func getDbConfig(name string) (host string, port string, db string, username string, password string, charset string, maxIdle int, maxConn int, err error) {

	// 读取配置文件
	cfg, err := config.ReadDefault(pathConfig)
	if err != nil {
		log.Error("读取配置文件出错" + err.Error())
		return "", "", "", "", "", "", 0, 0, err
	}

	// 取出配置项
	host, hostErr := cfg.String(name, "host")
	port, _ = cfg.String(name, "port")
	db, _ = cfg.String(name, "db")
	username, usernameErr := cfg.String(name, "username")
	password, passwordErr := cfg.String(name, "password")
	charset, charsetErr := cfg.String(name, "charset")
	maxIdle, maxIdleErr := cfg.Int(name, "maxIdle")
	maxConn, maxConnErr := cfg.Int(name, "maxConn")

	// 主配置项出错
	if hostErr != nil || usernameErr != nil || passwordErr != nil {
		log.Error("出错")
		return "", "", "", "", "", "", 0, 0, errors.New(name + "数据库主配置项为空")
	}

	// 可设置默认值配置项
	if charsetErr != nil {
		charset = "utf8mb4"
	}
	if maxIdleErr != nil {
		maxIdle = 8
	}
	if maxConnErr != nil {
		maxConn = 20
	}

	// 返回
	return host, port, db, username, password, charset, maxIdle, maxConn, nil
}
