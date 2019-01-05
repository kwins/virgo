package nsq

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/kwins/virgo/canal/core"

	"github.com/kwins/virgo/log"

	"github.com/nsqio/go-nsq"
)

const (
	MESSAGE_TYPE_JSON = int8(1)
)

// Producer 生产者
type Producer struct {
	count  uint64
	ps     map[string]*nsq.Producer
	length uint64
	addrs  []string
}

// NewProducer new producer
func NewProducer(addrs []string) *Producer {
	p := new(Producer)
	p.length = uint64(len(addrs))
	p.addrs = addrs
	p.ps = make(map[string]*nsq.Producer, p.length)
	cfg := nsq.NewConfig()
	for _, v := range addrs {
		producer, err := nsq.NewProducer(v, cfg)
		if err != nil {
			log.Fatalf("connect to nsq %s fail,detail=%s", v, err.Error())
		}
		p.ps[v] = producer
	}
	return p
}

func (producer *Producer) SendMsgJSON(topic string, v core.Record) error {
	return producer.SendMsg(topic, MESSAGE_TYPE_JSON, v)
}

func (producer *Producer) SendMsgsJSON(topic string, v ...core.Record) error {
	return producer.SendMsgs(topic, MESSAGE_TYPE_JSON, v...)
}

func (producer *Producer) SendMsg(topic string, messageType int8, v core.Record) error {
	idx := producer.count % producer.length
	addr := producer.addrs[idx]
	atomic.AddUint64(&producer.count, 1)
	body, err := fomart(v, messageType)
	if err != nil {
		return err
	}
	return producer.ps[addr].Publish(topic, body)
}

func (producer *Producer) SendMsgs(topic string, messageType int8, vs ...core.Record) error {
	idx := producer.count % producer.length
	addr := producer.addrs[idx]
	atomic.AddUint64(&producer.count, 1)

	var bodys = make([][]byte, len(vs))
	for i, v := range vs {
		body, err := fomart(v, messageType)
		if err != nil {
			return err
		}
		bodys[i] = body
	}
	return producer.ps[addr].MultiPublish(topic, bodys)
}

func (producer *Producer) Close() error {
	for _, v := range producer.ps {
		v.Stop()
	}
	return nil
}

func fomart(v core.Record, messageType int8) ([]byte, error) {
	switch messageType {
	case MESSAGE_TYPE_JSON:
		return json.Marshal(v)
	default:
		return nil, fmt.Errorf("unsupport message type:%d", messageType)
	}
}
