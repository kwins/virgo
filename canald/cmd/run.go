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
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kwins/virgo/canald/instance"
	"github.com/kwins/virgo/config"
	"github.com/kwins/virgo/export"
	"github.com/kwins/virgo/log"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile                string
	logLevel               string
	mysqlAddress           string
	mysqlUser              string
	mysqlPassword          string
	gtidon                 bool
	journalName            string
	position               int64
	canaldAddress          string
	canaldEventStoreSize   int64
	canaldFlushFrequency   int64
	canaldFlavor           string
	slaveID                int64
	canaldBatchSize        int64
	canaldExport           string
	etcdEndpoints          []string
	etcdUser               string
	etcdPassword           string
	etcdTimeout            int64
	elasticsearchHost      []string
	elasticsearchNamespace string
	elasticsearchDebug     bool
	elasticsearchUser      string
	elasticsearchPassword  string
	diskMasterDir          string
	nsqAddrs               []string
	nsqNamespace           string
	nsqDebug               bool
	nsqTopic               string
	kafkaAddrs             []string
	kafkaNamespace         string
	kafkaDebug             bool
	kafkaCertPath          string
	kafkaUser              string
	kafkaTopic             string
	kafakaPassword         string
	mysqlIncludeTableRegex []string
	mysqlExcludeTableRegex []string
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run your canald",
	Long: `Run command can start a canald server to listen mysql binlog event,
You can description config from config file or command line,example:

canald run -c=canald.toml 

If you use command line,please run canald run -h for help and command line parameter will overwrite corresponding config file`,

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
			cfg.LogLevel = viper.GetString("log_leve")
			cfg.Canald.Export = viper.GetString("canald.export")
			cfg.Canald.BatchSize = viper.GetInt64("canald.batch_size")
			cfg.Canald.Address = viper.GetString("canald.address")
			cfg.Canald.EventStoreSize = viper.GetInt64("canald.event_store_size")

			cfg.Mysql.SlaveID = uint32(viper.GetInt32("mysql.slave_id"))
			cfg.Mysql.Address = viper.GetString("mysql.address")
			cfg.Mysql.Charset = viper.GetString("mysql.charset")
			cfg.Mysql.Gtidon = viper.GetBool("mysql.gtidon")
			cfg.Mysql.JournaName = viper.GetString("mysql.journal_name")
			cfg.Mysql.Password = viper.GetString("mysql.password")
			cfg.Mysql.Position = viper.GetInt64("mysql.position")
			cfg.Mysql.User = viper.GetString("mysql.user")
			cfg.Mysql.IncludeTableRegex = viper.GetStringSlice("mysql.include_table_regex")
			cfg.Mysql.ExcludeTableRegex = viper.GetStringSlice("mysql.exclude_table_regex")

			cfg.Etcd.EndPoints = viper.GetStringSlice("etcd.endpoints")
			cfg.Etcd.Psw = viper.GetString("etcd.password")
			cfg.Etcd.User = viper.GetString("etcd.user")
			cfg.Etcd.Timeout = time.Duration(viper.GetInt64("etcd.timeout")) * time.Second

			cfg.Disk.MasterDir = viper.GetString("disk.master_dir")

			cfg.Elastic.Hosts = viper.GetStringSlice("elasticsearch.hosts")
			cfg.Elastic.Namespace = viper.GetString("elasticsearch.namespace")
			cfg.Elastic.Debug = viper.GetBool("elasticsearch.debug")
			cfg.Elastic.UserName = viper.GetString("elasticsearch.user")
			cfg.Elastic.Password = viper.GetString("elasticsearch.password")

			cfg.Nsq.Addrs = viper.GetStringSlice("nsq.addrs")
			cfg.Nsq.Namespace = viper.GetString("nsq.namespace")
			cfg.Nsq.Debug = viper.GetBool("nsq.debug")
			cfg.Nsq.Topic = viper.GetString("nsq.topic")

			cfg.Kafka.CertPath = viper.GetString("kafka.cert_path")
			cfg.Kafka.Addrs = viper.GetStringSlice("kafka.addrs")
			cfg.Kafka.Namespace = viper.GetString("kafka.namespace")
			cfg.Kafka.Debug = viper.GetBool("kafka.debug")
			cfg.Kafka.User = viper.GetString("kafka.user")
			cfg.Kafka.Password = viper.GetString("kafka.password")
			cfg.Kafka.Topic = viper.GetString("kafka.topic")

			cfg.Mns.Host = viper.GetString("mns.host")
			cfg.Mns.Namespace = viper.GetString("mns.namespace")
			cfg.Mns.QueueName = viper.GetString("mns.queue_name")
			cfg.Mns.AccessID = viper.GetString("mns.access_id")
			cfg.Mns.AccessSecret = viper.GetString("mns.access_secret")
			cfg.Mns.Debug = viper.GetBool("mns.debug")
		}

		if len(logLevel) > 0 {
			cfg.LogLevel = logLevel
		}
		log.SetLevelByName(cfg.LogLevel)
		if len(mysqlAddress) > 0 {
			cfg.Mysql.Address = mysqlAddress
		}
		if len(mysqlUser) > 0 {
			cfg.Mysql.User = mysqlUser
		}
		if len(mysqlPassword) > 0 {
			cfg.Mysql.Password = mysqlPassword
		}
		if gtidon {
			cfg.Mysql.Gtidon = gtidon
		}
		if len(journalName) > 0 {
			cfg.Mysql.JournaName = journalName
		}
		if position > 0 {
			cfg.Mysql.Position = position
		}
		if len(mysqlIncludeTableRegex) > 0 {
			cfg.Mysql.IncludeTableRegex = mysqlIncludeTableRegex
		}
		if len(mysqlExcludeTableRegex) > 0 {
			cfg.Mysql.ExcludeTableRegex = mysqlExcludeTableRegex
		}

		if err := cfg.Mysql.ArgumentValide(); err != nil {
			log.Error(err.Error())
			os.Exit(-1)
		}

		if len(canaldExport) > 0 {
			switch canaldExport {
			case export.CONSOLE, export.ELASTIC, export.KAFKA:
			default:
				log.Errorf("unsupport %s export, eg: console, elastic, kafaka", canaldExport)
				os.Exit(-1)
			}
			cfg.Canald.Export = canaldExport
		} else if len(cfg.Canald.Export) == 0 {
			log.Error("must be made a export,eg: console, elastic, kafaka")
			os.Exit(-1)
		}

		if len(canaldAddress) > 0 {
			cfg.Canald.Address = canaldAddress
		}
		if slaveID > 0 {
			cfg.Mysql.SlaveID = uint32(slaveID)
		}
		if canaldBatchSize > 0 {
			cfg.Canald.BatchSize = canaldBatchSize
		}
		if len(canaldFlavor) == 0 {
			cfg.Canald.Flavor = "mysql"
		} else {
			cfg.Canald.Flavor = canaldFlavor
		}
		if canaldFlushFrequency <= 0 {
			cfg.Canald.FlushFrequency = 3
		} else {
			cfg.Canald.FlushFrequency = time.Duration(canaldFlushFrequency)
		}

		if cfg.Canald.EventStoreSize <= 0 {
			if canaldEventStoreSize > 0 {
				cfg.Canald.EventStoreSize = canaldEventStoreSize
			} else {
				cfg.Canald.EventStoreSize = 2 << 15
			}
		}

		if len(etcdEndpoints) > 0 {
			cfg.Etcd.EndPoints = etcdEndpoints
			cfg.Etcd.User = etcdUser
			cfg.Etcd.Psw = etcdPassword
			cfg.Etcd.Timeout = time.Duration(etcdTimeout) * time.Second
		} else if len(diskMasterDir) > 0 {
			cfg.Disk.MasterDir = diskMasterDir
		}

		if len(cfg.Disk.MasterDir) == 0 {
			log.Errorf("must be made a master_dir argument")
			os.Exit(-1)
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

		// nsq
		if len(nsqAddrs) > 0 {
			cfg.Nsq.Addrs = nsqAddrs
		}
		if len(nsqNamespace) > 0 {
			cfg.Nsq.Namespace = nsqNamespace
		}
		if nsqDebug {
			cfg.Nsq.Debug = nsqDebug
		}
		if len(nsqTopic) > 0 {
			cfg.Nsq.Topic = nsqTopic
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

		sc := make(chan os.Signal, 1)
		signal.Notify(sc,
			os.Kill,
			os.Interrupt,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)

		done := make(chan struct{}, 1)
		c := instance.NewCanald(&cfg)

		go func() {
			c.Run()
			done <- struct{}{}
		}()

		select {
		case n := <-sc:
			log.Infof("receive signal %v, closing", n)
			c.Close()
		case <-c.Ctx().Done():
			log.Infof("context is done with %v, closing", c.Ctx().Err())
		}
		log.Infof("canald closed.")
		<-done
	},
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.AddCommand(runCmd)

	runCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.virgo.toml)")
	runCmd.PersistentFlags().StringVar(&logLevel, "log", "", "log level")

	// virgo run command line argments
	runCmd.PersistentFlags().StringVarP(&mysqlAddress, "mysql_address", "s", "", "msyql address")
	runCmd.PersistentFlags().StringVarP(&mysqlPassword, "mysql_password", "p", "", "")
	runCmd.PersistentFlags().StringVarP(&mysqlUser, "mysql_user", "u", "", "mysql user that must have replication privilege")
	runCmd.PersistentFlags().BoolVar(&gtidon, "gtidon", false, "mysql gtidon switch")
	runCmd.PersistentFlags().StringVarP(&journalName, "journal_name", "j", "", "mysql binlog file")
	runCmd.PersistentFlags().Int64Var(&position, "pos", 0, "the position of binlog that you want from mysql")
	runCmd.PersistentFlags().StringSliceVar(&mysqlIncludeTableRegex, "mysql_include_table_regex", nil, "mysql include table regex")
	runCmd.PersistentFlags().StringSliceVar(&mysqlExcludeTableRegex, "mysql_exclude_table_regex", nil, "mysql exclude table regex")

	runCmd.PersistentFlags().StringVar(&canaldExport, "export", "", "export data to")
	runCmd.PersistentFlags().StringVar(&canaldAddress, "canald_address", "", "virgo serve address")
	runCmd.PersistentFlags().Int64Var(&slaveID, "slave_id", 0, "mysql slave id")
	runCmd.PersistentFlags().Int64Var(&canaldBatchSize, "canald_batch_size", 0, "virgo batch get size")
	runCmd.PersistentFlags().Int64Var(&canaldFlushFrequency, "canald_flush_frequency", 1, "virgo flush frequency")
	runCmd.PersistentFlags().StringVar(&canaldFlavor, "canald_favor", "mysql", "virgo favor")
	runCmd.PersistentFlags().Int64Var(&canaldEventStoreSize, "canald_event_store_size", 0, " event store buffer size")

	runCmd.PersistentFlags().StringArrayVar(&etcdEndpoints, "etcd_endpoints", nil, "etcd endpoints")
	runCmd.PersistentFlags().StringVar(&etcdPassword, "etcd_password", "", "etcd password")
	runCmd.PersistentFlags().StringVar(&etcdUser, "etcd_user", "", "etcd user")
	runCmd.PersistentFlags().Int64Var(&etcdTimeout, "etcd_timeout", 0, "etcd connect timeout")

	runCmd.PersistentFlags().StringSliceVar(&elasticsearchHost, "es_hosts", nil, "elasticsearch hosts")
	runCmd.PersistentFlags().StringVar(&elasticsearchNamespace, "es_namespace", "", "elasticsearch namespace")
	runCmd.PersistentFlags().BoolVar(&elasticsearchDebug, "es_debug", false, "elasticsearch debug switch")
	runCmd.PersistentFlags().StringVar(&elasticsearchUser, "es_user", "", "elasticsearch user")
	runCmd.PersistentFlags().StringVar(&elasticsearchPassword, "es_password", "", "elasticsearch password")

	runCmd.PersistentFlags().StringVar(&diskMasterDir, "disk_master_dir", "", "disk master dir")

	runCmd.PersistentFlags().StringSliceVar(&nsqAddrs, "nsq_addrs", nil, "nsq hosts")
	runCmd.PersistentFlags().StringVar(&nsqNamespace, "nsq_namespace", "", "nsq namespace")
	runCmd.PersistentFlags().BoolVar(&nsqDebug, "nsq_debug", false, "nsq debug switch")
	runCmd.PersistentFlags().StringVar(&nsqTopic, "nsq_topic", "", "nsq producer topic")

	runCmd.PersistentFlags().StringVar(&kafkaCertPath, "kafka_cert_path", "", "kafka cert_path")
	runCmd.PersistentFlags().StringSliceVar(&kafkaAddrs, "kafka_addrs", nil, "kafka hosts")
	runCmd.PersistentFlags().StringVar(&kafkaNamespace, "kafka_namespace", "", "kafka namespace")
	runCmd.PersistentFlags().BoolVar(&kafkaDebug, "kafka_debug", false, "kafka debug switch")
	runCmd.PersistentFlags().StringVar(&kafkaUser, "kafka_user", "", "kafka user")
	runCmd.PersistentFlags().StringVar(&kafakaPassword, "kafka_password", "", "kafka password")
	runCmd.PersistentFlags().StringVar(&kafkaTopic, "kafka_topic", "", "kafka producer topic")

	runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetConfigType("toml")
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".virgo" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".virgo")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
