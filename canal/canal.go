package canal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/canal/master"
	"github.com/kwins/virgo/config"
	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/mysql"
	"github.com/kwins/virgo/mysql/client"
	"github.com/kwins/virgo/replication"
	"github.com/kwins/virgo/schema"
)

// Canal can sync your MySQL data into everywhere, like Elasticsearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
	m   sync.Mutex
	ctx context.Context
	cfg *config.Config

	p *sync.Pool
	// 事件获取
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer

	// Ringbuffer修改版本
	// 三个指针
	// GET：当前获取的最后一个位置，用于批量处理
	// PUT：最后一个放入的位置
	// ACK：最后一个处理完的时间
	// EventStore使用ACK机制，用来保证消息不丢
	store *EventStore

	// 当前binlog日志文件名称
	// 因为binlog内容里面除了日志轮转事件会有名称外，其他都没有名称
	// 所以需要暂存起来，方便后续保存
	curBinlogName string
	eventHandler  EventHandler

	connLock sync.Mutex
	conn     *client.Conn

	// 处理过的表缓存，用于快速查找
	tableLock sync.RWMutex
	tables    map[string]*schema.Table

	errorTablesGetTime map[string]time.Time

	tableMatchCache   map[string]bool
	includeTableRegex []*regexp.Regexp
	excludeTableRegex []*regexp.Regexp
}

// canal will retry fetching unknown table's meta after UnknownTableRetryPeriod
var UnknownTableRetryPeriod = time.Second * time.Duration(10)
var ErrExcludedTable = errors.New("excluded table meta")

func NewCanal(cfg *config.Config, api *clientv3.Client, ctx context.Context) (*Canal, error) {
	c := new(Canal)
	c.cfg = cfg
	c.ctx = ctx
	c.eventHandler = &DummyEventHandler{}
	c.tables = make(map[string]*schema.Table)
	if c.cfg.Mysql.DiscardNoMetaRowEvent {
		c.errorTablesGetTime = make(map[string]time.Time)
	}
	c.prepareStorer(api)
	var err error
	if err = c.prepareSyncer(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = c.checkBinlogRowFormat(); err != nil {
		return nil, errors.Trace(err)
	}
	c.p = &sync.Pool{
		New: func() interface{} {
			return new(core.Message)
		}}
	// init table filter
	if n := len(c.cfg.Mysql.IncludeTableRegex); n > 0 {
		c.includeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.Mysql.IncludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.includeTableRegex[i] = reg
		}
	}

	if n := len(c.cfg.Mysql.ExcludeTableRegex); n > 0 {
		c.excludeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.Mysql.ExcludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.excludeTableRegex[i] = reg
		}
	}

	if c.includeTableRegex != nil || c.excludeTableRegex != nil {
		c.tableMatchCache = make(map[string]bool)
	}

	return c, nil
}

func (c *Canal) prepareStorer(api *clientv3.Client) {
	var storer master.Store
	if len(c.cfg.Etcd.EndPoints) > 0 {
		storer = master.StoreBulder().EtcdStore(api)
		log.Info("canald use etcd as save postion information")
	} else if len(c.cfg.Disk.MasterDir) > 0 {
		log.Info("canald use config file as save postion information")
		storer = master.StoreBulder().DiskStore(c.cfg.Disk.MasterDir)
	} else { // 默认使用文件存储，存储位置为当前目录
		log.Info("canald use current dir file as save postion information")
		storer = master.StoreBulder().DiskStore(filepath.Dir(os.Args[0]))
	}
	c.store = NewEventStore(c.cfg.Canald.EventStoreSize, c.cfg.Canald.BatchSize,
		storer, c.cfg.Canald.FlushFrequency, c.ctx)
	log.Infof("canald start eventstore with size=%d batchsize=%d flush_frequency=%ds",
		c.cfg.Canald.EventStoreSize, c.cfg.Canald.BatchSize, c.cfg.Canald.FlushFrequency)
}

// RunFrom 存在master.info则使用其信息，否则从配置文件加载
// 未配置,使用mysql最后的位置
// 把位置信息缓存到内存，定时保存位置信息，减小因为保存位置带来的开销
func (c *Canal) RunFrom(pos mysql.Position) {
	if loadPos := c.store.master.S.Value(); len(loadPos.Name) > 0 {
		pos = loadPos
	} else {
		if len(pos.Name) == 0 {
			var err error
			pos, err = c.GetMasterPos()
			if err != nil {
				log.Error(err.Error())
				return
			}
		}
	}
	c.store.master.S.Save(pos)
	c.curBinlogName = pos.Name
	log.Infof("canal finally run from mysql postion %s", pos.String())
	c.runSyncBinlog()
}

// Ack ack
func (c *Canal) Ack(message *core.Message) error {
	c.p.Put(message)
	return c.store.Ack(message.BatchID())
}

// Get 从EventStore中获取消息
func (c *Canal) Get() *core.Message {
	message := c.p.Get().(*core.Message)
	message.Reset(c.store.Get())
	return message
}

// Rollback rollback
func (c *Canal) Rollback(message *core.Message) {
	c.p.Put(message)
	c.store.Rollback()
}

func (c *Canal) Close() {
	log.Info("canal closing...")

	c.m.Lock()
	defer c.m.Unlock()

	c.connLock.Lock()
	c.conn.Close()
	c.conn = nil
	c.connLock.Unlock()
	c.syncer.Close()
	// 刷新最后一次ACK的位置
	c.store.master.S.Flush()
	c.eventHandler.OnPosSynced(c.store.master.S.Value(), true)
	log.Info("canal closed.")
}

func (c *Canal) Ctx() context.Context {
	return c.ctx
}

func (c *Canal) checkTableMatch(key string) bool {
	// no filter, return true
	if c.tableMatchCache == nil {
		return true
	}

	c.tableLock.RLock()
	rst, ok := c.tableMatchCache[key]
	c.tableLock.RUnlock()
	if ok {
		// cache hit
		return rst
	}
	matchFlag := false
	// check include
	if c.includeTableRegex != nil {
		for _, reg := range c.includeTableRegex {
			if reg.MatchString(key) {
				matchFlag = true
				break
			}
		}
	}
	// check exclude
	if matchFlag && c.excludeTableRegex != nil {
		for _, reg := range c.excludeTableRegex {
			if reg.MatchString(key) {
				matchFlag = false
				break
			}
		}
	}
	c.tableLock.Lock()
	c.tableMatchCache[key] = matchFlag
	c.tableLock.Unlock()
	return matchFlag
}

func (c *Canal) GetTable(db string, table string) (*schema.Table, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	// if table is excluded, return error and skip parsing event or dump
	if !c.checkTableMatch(key) {
		return nil, ErrExcludedTable
	}
	c.tableLock.RLock()
	t, ok := c.tables[key]
	c.tableLock.RUnlock()

	if ok {
		return t, nil
	}

	if c.cfg.Mysql.DiscardNoMetaRowEvent {
		c.tableLock.RLock()
		lastTime, ok := c.errorTablesGetTime[key]
		c.tableLock.RUnlock()
		if ok && time.Now().Sub(lastTime) < UnknownTableRetryPeriod {
			return nil, schema.ErrMissingTableMeta
		}
	}

	t, err := schema.NewTable(c, db, table)
	if err != nil {
		// check table not exists
		if ok, err1 := schema.IsTableExist(c, db, table); err1 == nil && !ok {
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
			c.tableLock.Lock()
			c.tables[key] = ta
			c.tableLock.Unlock()
			return ta, nil
		}
		// if DiscardNoMetaRowEvent is true, we just log this error
		if c.cfg.Mysql.DiscardNoMetaRowEvent {
			c.tableLock.Lock()
			c.errorTablesGetTime[key] = time.Now()
			c.tableLock.Unlock()
			// log error and return ErrMissingTableMeta
			log.Errorf("canal get table meta err: %v", errors.Trace(err))
			return nil, schema.ErrMissingTableMeta
		}
		return nil, err
	}

	c.tableLock.Lock()
	c.tables[key] = t
	if c.cfg.Mysql.DiscardNoMetaRowEvent {
		// if get table info success, delete this key from errorTablesGetTime
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()

	return t, nil
}

// ClearTableCache clear table cache
func (c *Canal) ClearTableCache(db []byte, table []byte) {
	key := fmt.Sprintf("%s.%s", db, table)
	c.tableLock.Lock()
	delete(c.tables, key)
	if c.cfg.Mysql.DiscardNoMetaRowEvent {
		delete(c.errorTablesGetTime, key)
	}
	c.tableLock.Unlock()
}

// Check MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (c *Canal) CheckBinlogRowImage(image string) error {
	// need to check MySQL binlog row image? full, minimal or noblob?
	// now only log
	if c.cfg.Canald.Flavor == mysql.MySQLFlavor {
		if res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`); err != nil {
			return errors.Trace(err)
		} else {
			// MySQL has binlog row image from 5.6, so older will return empty
			rowImage, _ := res.GetString(0, 1)
			if rowImage != "" && !strings.EqualFold(rowImage, image) {
				return errors.Errorf("MySQL uses %s binlog row image, but we want %s", rowImage, image)
			}
		}
	}

	return nil
}

func (c *Canal) checkBinlogRowFormat() error {
	log.Info("checkout binlog row fromat start...")
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return errors.Errorf("binlog must ROW format, but %s now", f)
	}
	log.Info("checkout binlog row fromat ok~")
	return nil
}

func (c *Canal) prepareSyncer() error {
	seps := strings.Split(c.cfg.Mysql.Address, ":")
	if len(seps) != 2 {
		return errors.Errorf("invalid mysql addr format %s, must host:port", c.cfg.Mysql.Address)
	}

	port, err := strconv.ParseUint(seps[1], 10, 16)
	if err != nil {
		return errors.Trace(err)
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID:        c.cfg.Mysql.SlaveID,
		Flavor:          c.cfg.Canald.Flavor,
		Host:            seps[0],
		Port:            uint16(port),
		User:            c.cfg.Mysql.User,
		Password:        c.cfg.Mysql.Password,
		Charset:         c.cfg.Mysql.Charset,
		HeartbeatPeriod: c.cfg.Mysql.HeartbeatPeriod,
		ReadTimeout:     c.cfg.Mysql.ReadTimeout,
		UseDecimal:      c.cfg.Mysql.UseDecimal,
		SemiSyncEnabled: c.cfg.Mysql.SemiSyncEnabled,
	}

	c.syncer = replication.NewBinlogSyncer(cfg)
	return nil
}

// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			c.conn, err = client.Connect(c.cfg.Mysql.Address, c.cfg.Mysql.User, c.cfg.Mysql.Password, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = c.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			c.conn.Close()
			c.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (c *Canal) SyncedPosition() mysql.Position {
	return c.store.master.S.Value()
}
