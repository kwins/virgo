package canal

import (
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/kwins/virgo/mysql"
)

type DumpConfig struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string `toml:"mysqldump"`

	// Will override Databases, tables is in database table_db
	Tables  []string `toml:"tables"`
	TableDB string   `toml:"table_db"`

	Databases []string `toml:"dbs"`

	// Ignore table format is db.table
	IgnoreTables []string `toml:"ignore_tables"`

	// Dump only selected records. Quotes are mandatory
	Where string `toml:"where"`

	// If true, discard error msg, else, output to stderr
	DiscardErr bool `toml:"discard_err"`

	// Set true to skip --master-data if we have no privilege to do
	// 'FLUSH TABLES WITH READ LOCK'
	SkipMasterData bool `toml:"skip_master_data"`

	// Set to change the default max_allowed_packet size
	MaxAllowedPacketMB int `toml:"max_allowed_packet_mb"`
}

type Config struct {
	Addr     string `toml:"addr"`     // mysql
	User     string `toml:"user"`     // mysql
	Password string `toml:"password"` // mysql

	Charset         string        `toml:"charset"`          // mysql
	ServerID        uint32        `toml:"server_id"`        // canal
	Flavor          string        `toml:"flavor"`           // canal
	HeartbeatPeriod time.Duration `toml:"heartbeat_period"` //mysql
	ReadTimeout     time.Duration `toml:"read_timeout"`     // mysql

	// IncludeTableRegex or ExcludeTableRegex should contain database name
	// Only a table which matches IncludeTableRegex and dismatches ExcludeTableRegex will be processed
	// eg, IncludeTableRegex : [".*\\.canal"], ExcludeTableRegex : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'
	// Default IncludeTableRegex and ExcludeTableRegex are empty, this will include all tables
	IncludeTableRegex []string `toml:"include_table_regex"` // mysql
	ExcludeTableRegex []string `toml:"exclude_table_regex"` // mysql

	// discard row event without table meta
	DiscardNoMetaRowEvent bool `toml:"discard_no_meta_row_event"` // mysql

	Dump DumpConfig `toml:"dump"`

	UseDecimal bool `toml:"use_decimal"` // msyql

	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool `toml:"semi_sync_enabled"`

	// 新增
	BatchSize int64 `toml:"batch_size"`

	// 事件缓存大小
	EventStoreSize int64 `toml:"event_store_size"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

func NewDefaultConfig() *Config {
	c := new(Config)

	c.Addr = "127.0.0.1:3306"
	c.User = "root"
	c.Password = ""

	c.Charset = mysql.DEFAULT_CHARSET
	rand.Seed(time.Now().Unix())
	c.ServerID = uint32(rand.Intn(1000)) + 1001

	c.Flavor = "mysql"

	c.Dump.ExecutionPath = "mysqldump"
	c.Dump.DiscardErr = true
	c.Dump.SkipMasterData = false

	return c
}