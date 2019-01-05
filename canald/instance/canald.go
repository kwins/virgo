package instance

import (
	"context"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/kwins/virgo/canal"
	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/config"
	"github.com/kwins/virgo/export"
	"github.com/kwins/virgo/export/console"
	"github.com/kwins/virgo/export/elastic"
	"github.com/kwins/virgo/export/kafaka"
	"github.com/kwins/virgo/export/mns"
	"github.com/kwins/virgo/export/nsq"
	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/mysql"
	"github.com/kwins/virgo/replication"
	"google.golang.org/grpc"
)

// Canald canald is a manager
type Canald struct {
	cfg    *config.Config
	ctx    context.Context
	cancel context.CancelFunc
	out    export.Bulk

	// canal 负责和mysql通信，解析协议
	// 并提供获取事件，确认事件消费接口
	c *canal.Canal

	// guard 负责主从实例的管理，自动切换
	// 当配置Etcd则认为部署主从
	guard *Guard

	etcdapi *clientv3.Client
}

func NewCanald(cfg *config.Config) *Canald {
	canald := new(Canald)
	canald.cfg = cfg

	switch strings.ToLower(canald.cfg.Canald.Export) {
	case export.CONSOLE:
		canald.out = console.NewExport()
	case export.ELASTIC:
		canald.out = elastic.NewExport(cfg.Elastic)
	case export.KAFKA:
		canald.out = kafaka.NewExport(cfg.Kafka)
	case export.NSQ:
		canald.out = nsq.NewExport(cfg.Nsq)
	case export.MNS:
		canald.out = mns.NewExport(cfg.Mns)
	default:
		log.Fatalf("unsupport exporter:%s", canald.cfg.Canald.Export)
	}

	log.Infof("canald use %s as exporter", canald.cfg.Canald.Export)

	canald.ctx, canald.cancel = context.WithCancel(context.Background())
	if len(canald.cfg.Etcd.EndPoints) > 0 {
		canald.readyGuard(cfg)
	}
	return canald
}

// Run
func (canald *Canald) Run() {
	if canald.guard != nil {
		if err := canald.guard.Try(eNode{
			CanaldID: canald.cfg.Canald.ID,
			Address:  canald.cfg.Canald.Address,
		}); err != nil {
			log.Error(err.Error())
		}
	}
	var err error
	canald.c, err = canal.NewCanal(canald.cfg, canald.etcdapi, canald.ctx)
	if err != nil {
		panic(err.Error())
	}
	canald.c.SetEventHandler(canald)

	go canald.onMessage(canald.ctx)
	// 同步位置信息优先级
	// 1：配置文件的
	// 2：磁盘或者远端存储的
	// 3：mysql当前的
	log.Infof("canald start run from journaName=%s position=%d", canald.cfg.Mysql.JournaName, canald.cfg.Mysql.Position)
	canald.c.RunFrom(mysql.Position{
		Name: canald.cfg.Mysql.JournaName,
		Pos:  uint32(canald.cfg.Mysql.Position),
	})
}

func (canald *Canald) readyGuard(cfg *config.Config) {
	var err error
	canald.etcdapi, err = clientv3.New(clientv3.Config{

		Endpoints: canald.cfg.Etcd.EndPoints,
		Username:  canald.cfg.Etcd.User,
		Password:  canald.cfg.Etcd.Psw,

		DialOptions: []grpc.DialOption{
			grpc.WithTimeout(time.Second * 3),
			grpc.WithInsecure(),
		},

		DialTimeout: time.Second * canald.cfg.Etcd.Timeout,
	})
	if err != nil {
		panic(err.Error())
	}
	canald.guard = NewGuard(canald.etcdapi, canald.ctx)
}

// onMessage 处理消息
func (canald *Canald) onMessage(ctx context.Context) {
	for {
		messages := canald.c.Get()
		if messages.BatchID() == -1 || messages.Len() == 0 {
			time.Sleep(time.Second * 1)
			continue
		}
		canald.processMessages(messages)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// processMessages 处理到来的消息实体
func (canald *Canald) processMessages(messages *core.Message) {
	log.Debugf("batchID=%d", messages.BatchID())
	var err error
	for _, messgae := range messages.Events() {
		switch ev := messgae.(type) {
		case *core.RowsEvent: // 只关心 ROWSEVENT 事件
			switch ev.Action {
			case core.ACTION_INSERT:
				err = canald.out.INSERT(ev)
			case core.ACTION_UPDATE:
				err = canald.out.UPDATE(ev)
			case core.ACTION_DELETE:
				err = canald.out.DELETE(ev)
			}
		}
	}

	if err != nil {
		log.Error(err.Error())
		canald.c.Rollback(messages)
	} else {
		if err = canald.c.Ack(messages); err != nil {
			log.Error(err.Error())
		}
	}
}

// Ctx return ctx
func (canald *Canald) Ctx() context.Context {
	return canald.ctx
}

// Close close canald
func (canald *Canald) Close() {
	canald.cancel()
	if canald.c != nil {
		canald.c.Close()
	}
	if canald.etcdapi != nil {
		canald.etcdapi.Close()
	}
	canald.out.CLOSE()
}

// OnRotate binglog 文件变化
func (canald *Canald) OnRotate(e *replication.RotateEvent) error {
	return nil
}

// OnTableChanged binglog 表变化
func (canald *Canald) OnTableChanged(schema, table string) error {
	log.Info(schema, "   ", table)
	return nil
}

func (canald *Canald) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	log.Info(nextPos)
	return nil
}

func (canald *Canald) OnXID(nextPos mysql.Position) error {
	log.Info(nextPos)
	return nil
}

// OnRow 数据变更
func (canald *Canald) OnRow(e *core.RowsEvent) error {
	log.Info(e.String())
	return nil
}

func (canald *Canald) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (canald *Canald) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (canald *Canald) String() string {
	return "Canald"
}
