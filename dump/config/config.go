package config

import (
	"github.com/kwins/virgo/config"
)

type MysqlCfg struct {
	Address           string   `toml:"address"`
	User              string   `toml:"user"`
	Password          string   `toml:"password"`
	Charset           string   `toml:"charset"`
	IncludeTableRegex []string `toml:"include_table_regex"`
	ExcludeTableRegex []string `toml:"exclude_table_regex"`
}

// DumpCfg dump config
type DumpCfg struct {
	Export            string   `toml:"export"` // 数据输出位置
	MysqldumpPath     string   `toml:"mysqldump"`
	Where             string   `toml:"where"`
	MasterDataSkipped bool     `toml:"master_data_skipped"`
	Databases         []string `toml:"databases"`
	TableDB           string   `toml:"table_db"`
	Tables            []string `toml:"tables"`
	MasterDir         string   `toml:"master_dir"`
}

// Config config
type Config struct {
	Mysql   MysqlCfg                `toml:"mysql"`
	Dump    DumpCfg                 `toml:"dump"`
	Elastic config.ElasticSearchCfg `toml:"elastic"`
	Kafka   config.KafkaCfg         `toml:"kafka"`
}
