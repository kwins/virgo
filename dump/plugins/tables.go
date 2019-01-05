package plugins

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/kwins/virgo/dump/config"
	"github.com/kwins/virgo/mysql"
	"github.com/kwins/virgo/mysql/client"
	"github.com/kwins/virgo/schema"
)

// Tables table 缓存
type Tables struct {
	cfg      config.MysqlCfg
	connLock sync.Mutex
	conn     *client.Conn

	// 处理过的表缓存，用于快速查找
	tableLock sync.RWMutex
	caches    map[string]*schema.Table
}

// NewTables new tables
func NewTables(cfg config.MysqlCfg) *Tables {
	tables := new(Tables)
	tables.cfg = cfg
	tables.caches = make(map[string]*schema.Table)
	return tables
}

// Execute a SQL,首次创建连接
func (tables *Tables) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	tables.connLock.Lock()
	defer tables.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if tables.conn == nil {
			tables.conn, err = client.Connect(tables.cfg.Address, tables.cfg.User, tables.cfg.Password, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = tables.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			tables.conn.Close()
			tables.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

// GetTable get table
func (tables *Tables) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	tables.tableLock.RLock()
	t, ok := tables.caches[key]
	tables.tableLock.RUnlock()

	if ok {
		return t, nil
	}

	t, err := schema.NewTable(tables, db, table)
	if err != nil {
		// check table not exists
		if ok, err1 := schema.IsTableExist(tables, db, table); err1 == nil && !ok {
			return nil, schema.ErrTableNotExist
		}
		// work around : RDS HAHeartBeat
		// ref : https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L385
		// issue : https://github.com/alibaba/canal/issues/222
		// This is a common error in RDS that canal can't get HAHealthCheckSchema's meta, so we mock a table meta.
		// If canal just skip and log error, as RDS HA heartbeat interval is very short, so too many HAHeartBeat errors will be logged.
		if key == schema.HAHealthCheckSchema {
			// mock ha_health_check meta
			ta := &schema.Table{
				Schema:  db,
				Name:    table,
				Columns: make([]schema.TableColumn, 0, 2),
				Indexes: make([]*schema.Index, 0),
			}
			ta.AddColumn("id", "bigint(20)", "", "")
			ta.AddColumn("type", "char(1)", "", "")
			tables.tableLock.Lock()
			tables.caches[key] = ta
			tables.tableLock.Unlock()
			return ta, nil
		}
		return nil, err
	}

	tables.tableLock.Lock()
	tables.caches[key] = t
	tables.tableLock.Unlock()
	return t, nil
}

// GetMasterPos GetMasterPos
func (tables *Tables) GetMasterPos() (mysql.Position, error) {
	rr, err := tables.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{Name: "", Pos: 0}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{
		Name: name,
		Pos:  uint32(pos),
	}, nil
}
