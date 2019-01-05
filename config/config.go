package config

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// ElasticSearchCfg elastic
type ElasticSearchCfg struct {
	Hosts     []string `toml:"hosts" json:"hosts"`
	Namespace string   `toml:"namespace" json:"namespace"` // index前缀
	UserName  string   `toml:"user" json:"user"`
	Password  string   `toml:"password" json:"password"`
	Debug     bool     `toml:"debug" json:"debug"` // 开启ES debug
}

// KafkaCfg kafka
type KafkaCfg struct {
	CertPath  string   `toml:"cert_path" json:"cert_path"`
	Addrs     []string `toml:"addrs" json:"addrs"`
	Topic     string   `toml:"topic" json:"topic"` // 发送到哪个topic
	Namespace string   `toml:"namespace" json:"namespace"`
	User      string   `toml:"user" json:"user"`
	Password  string   `toml:"password" json:"password"`
	GroupID   string   `toml:"group_id" json:"group_id"` // 消费端使用
	Debug     bool     `toml:"debug" json:"debug"`       // 开启ES debug
}

// KafkaConsumerCfg kafka
type KafkaConsumerCfg struct {
	CertPath  string   `toml:"cert_path" json:"cert_path"`
	Addrs     []string `toml:"addrs" json:"addrs"`
	Namespace string   `toml:"namespace" json:"namespace"`
	User      string   `toml:"user" json:"user"`
	Password  string   `toml:"password" json:"password"`
	GroupID   string   `toml:"group_id" json:"group_id"` // 消费端使用
	Topics    []string `toml:"topics" json:"topics"`
	Partition int64    `toml:"partition" json:"partition"` // 分区
	Debug     bool     `toml:"debug" json:"debug"`         // 开启ES debug
}

// NsqCfg nsq config
type NsqCfg struct {
	Addrs     []string `toml:"addrs" json:"addrs"`
	Namespace string   `toml:"namespace" json:"namespace"`
	Topic     string   `toml:"topic" json:"topic"` // 发送到哪个topic
	Debug     bool     `toml:"debug" json:"debug"` // 开启ES debug
}

// MnsCfg mns config
type MnsCfg struct {
	Namespace    string `toml:"namespace" json:"namespace"`
	Host         string `toml:"host" json:"host"`
	QueueName    string `toml:"queue_name" json:"queue_name"`
	AccessID     string `toml:"access_id" json:"access_id"`
	AccessSecret string `toml:"access_secret" json:"access_secret"`
	Debug        bool   `toml:"debug" json:"debug"`
}

// MysqlCfg mysql config
type MysqlCfg struct {
	SlaveID    uint32 `toml:"slave_id" json:"slave_id"`
	Address    string `toml:"address" json:"address"`
	User       string `toml:"user" json:"user"`
	Password   string `toml:"password" json:"password"`
	Charset    string `toml:"charset" json:"charset"`
	Gtidon     bool   `toml:"gtidon" json:"gtidon"`
	JournaName string `toml:"journal_name" json:"journal_name"`
	Position   int64  `toml:"position" json:"position"`

	// IncludeTableRegex or ExcludeTableRegex should contain database name
	// Only a table which matches IncludeTableRegex and dismatches ExcludeTableRegex will be processed
	// eg, IncludeTableRegex : [".*\\.canal"], ExcludeTableRegex : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'
	// Default IncludeTableRegex and ExcludeTableRegex are empty, this will include all tables
	IncludeTableRegex []string `toml:"include_table_regex" json:"include_table_regex"`
	ExcludeTableRegex []string `toml:"exclude_table_regex" json:"exclude_table_regex"`

	HeartbeatPeriod       time.Duration `toml:"heartbeat_period" json:"heartbeat_period"`
	ReadTimeout           time.Duration `toml:"read_timeout" json:"read_timeout"`
	DiscardNoMetaRowEvent bool          `toml:"discard_no_meta_row_event" json:"discard_no_meta_row_event"`
	UseDecimal            bool          `toml:"use_decimal" json:"use_decimal"`

	// SemiSyncEnabled enables semi-sync or not.半同步复制
	SemiSyncEnabled bool `toml:"semi_sync_enabled"`
}

// ArgumentValide mysql config valide
func (cfg MysqlCfg) ArgumentValide() error {
	if cfg.SlaveID <= 0 {
		return fmt.Errorf("mysql slave_id argument can't %d", cfg.SlaveID)
	}
	if cfg.Address == "" {
		return fmt.Errorf("mysql address argument can't \"\"")
	}
	if cfg.User == "" {
		return fmt.Errorf("mysql user argument can't \"\"")
	}
	if cfg.Password == "" {
		return fmt.Errorf("mysql password argument can't \"\"")
	}
	if cfg.Charset == "" {
		return fmt.Errorf("mysql charset argument can't \"\"")
	}
	return nil
}

// CanaldCfg virgo config
type CanaldCfg struct {
	ID             uint16        `toml:"id" json:"id"`
	Export         string        `toml:"export" json:"export"`
	Flavor         string        `toml:"flavor" json:"flavor"`
	Address        string        `toml:"address" json:"address"`
	BatchSize      int64         `toml:"batch_size" json:"batch_size"`
	EventStoreSize int64         `toml:"event_store_size" json:"event_store_size"`
	FlushFrequency time.Duration `toml:"flush_frequency" json:"flush_frequency"` // 存储master节点位置自动刷新频率
}

// ArgumentValide argument is valide
func (cfg CanaldCfg) ArgumentValide() error {
	if cfg.ID <= 0 {
		return fmt.Errorf("virgo id argument must greate %d", 0)
	}
	if cfg.Flavor != "mysql" {
		return fmt.Errorf("virgo flavor argument only support %s", "mysql")
	}
	// if cfg.Address == "" {
	// 	return fmt.Errorf("virgo address argument can't \"\"")
	// }
	return nil
}

// EtcdCfg etcd config
type EtcdCfg struct {
	EndPoints []string      `toml:"endpoints" json:"endpoints"`
	User      string        `toml:"user" json:"user"`
	Psw       string        `toml:"passwrod" json:"passwrod"`
	Timeout   time.Duration `toml:"timeout" json:"timeout"`
}

type DiskCfg struct {
	MasterDir string `toml:"master_dir" json:"master_dir"`
}

// Config config
type Config struct {
	LogLevel string           `toml:"log_level" json:"log_level"`
	Mysql    MysqlCfg         `toml:"mysql" json:"mysql"`
	Canald   CanaldCfg        `toml:"virgo" json:"virgo"`
	Etcd     EtcdCfg          `toml:"etcd" json:"etcd"`
	Disk     DiskCfg          `toml:"disk" json:"disk"`
	Elastic  ElasticSearchCfg `toml:"elastic" json:"elastic"`
	Kafka    KafkaCfg         `toml:"kafka" json:"kafka"`
	Nsq      NsqCfg           `toml:"nsq" json:"nsq"`
	Mns      MnsCfg           `toml:"mns" json:"mns"`
}

// NewConfig new config
func NewConfig(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var cfg Config
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		return nil, errors.Trace(err)
	}
	return &cfg, nil
}
