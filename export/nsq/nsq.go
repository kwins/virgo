package nsq

import (
	"github.com/kwins/virgo/config"

	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/log"
)

// Export export data
// NSQ 本身消息不是保证顺序的，同步的数据也不是有序的
type Export struct {
	cfg      config.NsqCfg
	producer *Producer
}

// NewExport new export
func NewExport(cfg config.NsqCfg) *Export {
	exp := new(Export)
	exp.cfg = cfg
	exp.producer = NewProducer(cfg.Addrs)
	return exp
}

// INSERT insert data to nsq
func (export *Export) INSERT(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// UPDATE update data to nsq
func (export *Export) UPDATE(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// DELETE delete data to nsq
func (export *Export) DELETE(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// CONSOLE print data to console
func (export *Export) CONSOLE(ev *core.RowsEvent) {
	log.Info(ev.String())
}

func (export *Export) sendMsg(ev *core.RowsEvent) error {
	records := ev.Records(export.cfg.Namespace)
	if export.cfg.Debug {
		for _, v := range records {
			log.Info(v.String())
		}
	}
	return export.producer.SendMsgsJSON(export.cfg.Topic, records...)
}

// CLOSE close
func (export *Export) CLOSE() error {
	return export.producer.Close()
}
