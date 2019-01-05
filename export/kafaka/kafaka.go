package kafaka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/config"
	"github.com/kwins/virgo/log"
)

// Export export data
type Export struct {
	num      uint64
	cfg      config.KafkaCfg
	producer sarama.SyncProducer
}

// NewExport new export
func NewExport(cfg config.KafkaCfg) *Export {
	var exp = new(Export)
	exp.cfg = cfg
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner

	// 走认证
	if len(cfg.User) > 0 || len(cfg.Password) > 0 {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.User
		config.Net.SASL.Password = cfg.Password
		config.Net.SASL.Handshake = true
		certBytes, err := ioutil.ReadFile(cfg.CertPath)
		if err != nil {
			log.Fatal(err.Error())
		}
		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(certBytes); !ok {
			log.Fatal("kafka producer failed to parse root certificate")
		}
		config.Net.TLS.Config = &tls.Config{
			RootCAs:            certPool,
			InsecureSkipVerify: true,
		}
		config.Net.TLS.Enable = true
	}

	var err error
	if err = config.Validate(); err != nil {
		log.Fatal(err.Error())
	}
	exp.producer, err = sarama.NewSyncProducer(cfg.Addrs, config)
	if err != nil {
		log.Fatal(err.Error())
	}
	return exp
}

// INSERT insert data to elastic
func (export *Export) INSERT(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// UPDATE update data to elastic
func (export *Export) UPDATE(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// DELETE delete data to elastic
func (export *Export) DELETE(ev *core.RowsEvent) error {
	return export.sendMsg(ev)
}

// CONSOLE print data to console
func (export *Export) CONSOLE(ev *core.RowsEvent) {
	for _, v := range ev.Records(export.cfg.Namespace) {
		log.Info(v.String())
	}
}

func (export *Export) sendMsg(ev *core.RowsEvent) error {
	records := ev.Records(export.cfg.Namespace)
	if export.cfg.Debug {
		for _, v := range records {
			log.Info(v.String())
		}
	}
	var messages = make([]*sarama.ProducerMessage, len(records))
	atomic.AddUint64(&export.num, uint64(len(records)))
	for i, v := range records {
		b, err := json.Marshal(&v)
		if err != nil {
			return err
		}
		var pk string
		var val map[string]interface{}
		switch v.Action {
		case core.ACTION_DELETE, core.ACTION_INSERT:
			val = v.Columns[0]
		case core.ACTION_UPDATE:
			val = v.Columns[1]
		default:
			log.Errorf("bad action:%s", v.Action)
			return nil
		}

		if len(v.PkNames[0]) > 0 {
			pk = fmt.Sprintf("%v", val[v.PkNames[0]])
		} else {
			pk = ev.Table.Name
		}

		messages[i] = &sarama.ProducerMessage{
			Topic: export.cfg.Topic,
			Key:   sarama.StringEncoder(pk),
			Value: sarama.ByteEncoder(b),
		}
		log.Debugf("message %d addr=%p", i, &messages[i])
	}
	return export.producer.SendMessages(messages)
}

// CLOSE close
func (export *Export) CLOSE() error {
	return export.producer.Close()
}
