// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"

	"github.com/kwins/virgo/dump"
	"github.com/kwins/virgo/dump/config"
	"github.com/kwins/virgo/dump/plugins"
	"github.com/kwins/virgo/export"
	"github.com/kwins/virgo/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	mysqlAddress           string
	mysqlUser              string
	mysqlPassword          string
	mysqlCharset           string
	dumpExport             string
	dumpMysqldumpPath      string
	dumpWhere              string
	dumpMasterDataSkipped  bool
	dumpDatabases          []string
	dumpTableDB            string
	dumpTables             []string
	dumpMasterDir          string
	elasticsearchHost      []string
	elasticsearchNamespace string
	elasticsearchDebug     bool
	elasticsearchUser      string
	elasticsearchPassword  string
	kafkaAddrs             []string
	kafkaNamespace         string
	kafkaDebug             bool
	kafkaCertPath          string
	kafkaUser              string
	kafakaPassword         string
	kafkaTopic             string
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "dump mysql data to plugins",
	Long: `
dump is use mysqldump ctl dump data from mysql to plugins,we now support plugins elasticsearch`,
	Run: func(cmd *cobra.Command, args []string) {
		var cfg config.Config
		if len(cfgFile) > 0 {
			f, err := os.Open(cfgFile)
			if err != nil {
				panic(err.Error())
			}
			if err := viper.ReadConfig(f); err != nil {
				panic(err.Error())
			}
			cfg.Mysql.Address = viper.GetString("mysql.address")
			cfg.Mysql.User = viper.GetString("mysql.user")
			cfg.Mysql.Password = viper.GetString("mysql.password")
			cfg.Mysql.Charset = viper.GetString("mysql.charset")

			cfg.Dump.Export = viper.GetString("dump.export")
			cfg.Dump.Databases = viper.GetStringSlice("dump.databases")
			cfg.Dump.MasterDataSkipped = viper.GetBool("dump.master_data_skipped")
			cfg.Dump.MysqldumpPath = viper.GetString("dump.mysqldump")
			cfg.Dump.TableDB = viper.GetString("dump.table_db")
			cfg.Dump.Tables = viper.GetStringSlice("dump.tables")
			cfg.Dump.Where = viper.GetString("dump.where")
			cfg.Dump.MasterDir = viper.GetString("dump.master_dir")
			cfg.Elastic.Hosts = viper.GetStringSlice("elasticsearch.hosts")
			cfg.Elastic.Namespace = viper.GetString("elasticsearch.namespace")
			cfg.Elastic.Debug = viper.GetBool("elasticsearch.debug")
			cfg.Elastic.UserName = viper.GetString("elasticsearch.user")
			cfg.Elastic.Password = viper.GetString("elasticsearch.password")

			cfg.Kafka.CertPath = viper.GetString("kafka.cert_path")
			cfg.Kafka.Addrs = viper.GetStringSlice("kafka.addrs")
			cfg.Kafka.Namespace = viper.GetString("kafka.namespace")
			cfg.Kafka.Debug = viper.GetBool("kafka.debug")
			cfg.Kafka.User = viper.GetString("kafka.user")
			cfg.Kafka.Password = viper.GetString("kafka.password")
			cfg.Kafka.Topic = viper.GetString("kafka.topic")
		}

		if len(mysqlAddress) > 0 {
			cfg.Mysql.Address = mysqlAddress
		}
		if len(mysqlUser) > 0 {
			cfg.Mysql.User = mysqlUser
		}
		if len(mysqlPassword) > 0 {
			cfg.Mysql.Password = mysqlPassword
		}
		if len(mysqlCharset) > 0 {
			cfg.Mysql.Charset = mysqlCharset
		}

		if len(dumpExport) > 0 {
			switch dumpExport {
			case export.CONSOLE, export.ELASTIC, export.KAFKA:
			default:
				log.Errorf("unsupport %s export, eg: console, elastic, kafaka", dumpExport)
				os.Exit(-1)
			}
			cfg.Dump.Export = dumpExport
		} else if len(cfg.Dump.Export) == 0 {
			log.Error("must be made a export, eg: console, elastic, kafaka")
			os.Exit(-1)
		}

		if len(dumpDatabases) > 0 {
			cfg.Dump.Databases = dumpDatabases
		}
		if dumpMasterDataSkipped {
			cfg.Dump.MasterDataSkipped = dumpMasterDataSkipped
		}
		if len(dumpMysqldumpPath) > 0 {
			cfg.Dump.MysqldumpPath = dumpMysqldumpPath
		}
		if len(dumpTableDB) > 0 {
			cfg.Dump.TableDB = dumpTableDB
		}
		if len(dumpTables) > 0 {
			cfg.Dump.Tables = dumpTables
		}
		if len(dumpWhere) > 0 {
			cfg.Dump.Where = dumpWhere
		}
		if len(dumpMasterDir) > 0 {
			cfg.Dump.MasterDir = dumpMasterDir
		}

		// elastic
		if len(elasticsearchHost) > 0 {
			cfg.Elastic.Hosts = elasticsearchHost
		}

		if len(elasticsearchNamespace) > 0 {
			cfg.Elastic.Namespace = elasticsearchNamespace
		}
		if elasticsearchDebug {
			cfg.Elastic.Debug = elasticsearchDebug
		}
		if len(elasticsearchUser) > 0 {
			cfg.Elastic.UserName = elasticsearchUser
		}
		if len(elasticsearchPassword) > 0 {
			cfg.Elastic.Password = elasticsearchPassword
		}

		// kafka
		if len(kafkaAddrs) > 0 {
			cfg.Kafka.Addrs = kafkaAddrs
		}
		if len(kafkaNamespace) > 0 {
			cfg.Kafka.Namespace = kafkaNamespace
		}
		if kafkaDebug {
			cfg.Kafka.Debug = kafkaDebug
		}
		if len(kafkaCertPath) > 0 {
			cfg.Kafka.CertPath = kafkaCertPath
		}
		if len(kafkaUser) > 0 {
			cfg.Kafka.User = kafkaUser
		}
		if len(kafakaPassword) > 0 {
			cfg.Kafka.Password = kafakaPassword
		}
		if len(kafkaTopic) > 0 {
			cfg.Kafka.Topic = kafkaTopic
		}
		dumper, err := dump.NewDumper(cfg)
		if err != nil {
			log.Error(err.Error())
			os.Exit(-1)
		}

		h := plugins.NewDefaultDumpHandler(cfg)
		if err = dumper.DumpAndParse(h); err != nil {
			log.Error(err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)

	dumpCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")

	dumpCmd.PersistentFlags().StringVar(&mysqlAddress, "mysql_address", "", "mysql address")
	dumpCmd.PersistentFlags().StringVar(&mysqlUser, "mysql_user", "", "mysql user")
	dumpCmd.PersistentFlags().StringVar(&mysqlPassword, "mysql_password", "", "mysql password")
	dumpCmd.PersistentFlags().StringVar(&mysqlCharset, "mysql_charset", "", "mysql charset")

	dumpCmd.PersistentFlags().StringVar(&dumpExport, "export", "", "dump to export")
	dumpCmd.PersistentFlags().StringVar(&dumpMysqldumpPath, "mysqldump_path", "", "default mysqldump path")
	dumpCmd.PersistentFlags().StringVar(&dumpWhere, "dump_where", "", "dump where condition")
	dumpCmd.PersistentFlags().BoolVar(&dumpMasterDataSkipped, "dump_master_data_skipped", false, "dump master data skipped, default false")
	dumpCmd.PersistentFlags().StringSliceVar(&dumpDatabases, "dump_databases", nil, "dump databases")
	dumpCmd.PersistentFlags().StringVar(&dumpTableDB, "dump_table_db", "", "dump table db")
	dumpCmd.PersistentFlags().StringSliceVar(&dumpTables, "dump_tables", nil, "dump tables")
	dumpCmd.PersistentFlags().StringVar(&dumpMasterDir, "dump_master_dir", "", "dump master dir")

	dumpCmd.PersistentFlags().StringSliceVar(&elasticsearchHost, "es_hosts", nil, "elasticsearch hosts")
	dumpCmd.PersistentFlags().StringVar(&elasticsearchNamespace, "es_namespace", "", "elasticsearch namespace")
	dumpCmd.PersistentFlags().BoolVar(&elasticsearchDebug, "es_debug", false, "elasticsearch debug switch")
	dumpCmd.PersistentFlags().StringVar(&elasticsearchUser, "es_user", "", "elasticsearch user")
	dumpCmd.PersistentFlags().StringVar(&elasticsearchPassword, "es_password", "", "elasticsearch password")

	dumpCmd.PersistentFlags().StringVar(&kafkaCertPath, "kafka_cert_path", "", "kafka group")
	dumpCmd.PersistentFlags().StringSliceVar(&kafkaAddrs, "kafka_addrs", nil, "kafka hosts")
	dumpCmd.PersistentFlags().StringVar(&kafkaNamespace, "kafka_namespace", "", "kafka namespace")
	dumpCmd.PersistentFlags().BoolVar(&kafkaDebug, "kafka_debug", false, "kafka debug switch")
	dumpCmd.PersistentFlags().StringVar(&kafkaUser, "kafka_user", "", "kafka user")
	dumpCmd.PersistentFlags().StringVar(&kafakaPassword, "kafka_password", "", "kafka password")
	dumpCmd.PersistentFlags().StringVar(&kafkaTopic, "kafka_topic", "", "kafka producer topic")
}
