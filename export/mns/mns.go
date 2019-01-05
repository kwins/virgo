package mns

import (
	"encoding/json"

	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/config"
	"github.com/kwins/virgo/log"
)

// Export export data
type Export struct {
	cfg      config.MnsCfg
	producer *Producer
}

// NewExport new export
func NewExport(cfg config.MnsCfg) *Export {
	exp := new(Export)
	exp.cfg = cfg
	exp.producer = NewProducer(cfg)
	return exp
}

// INSERT insert data to mns
func (export *Export) INSERT(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// UPDATE update data to mns
func (export *Export) UPDATE(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// DELETE delete data to mns
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

	var message = make([]Message, len(records))
	for i := range records {
		b, err := json.Marshal(&records[i])
		if err != nil {
			return err
		}
		message[i].MessageBody = b
		message[i].DelaySeconds = 0
		message[i].Priority = 8
	}
	return export.producer.SendMsgs(export.cfg.QueueName, message...)
}

// CLOSE close
func (export *Export) CLOSE() error {
	return nil
}
