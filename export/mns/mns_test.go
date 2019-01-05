package mns

import (
	"testing"
)

func TestMNS(t *testing.T) {
	// produer := NewProducer(config.MnsCfg{
	// 	Namespace:    "play",
	// 	Host:         "https://31318278.mns.cn-hangzhou.aliyuncs.com",
	// 	QueueName:    "play-event-qa",
	// 	AccessID:     "LTAIptdGl4O3wVq2",
	// 	AccessSecret: "0YsvICco2gLngkRv8ScECvpeuFaASE",
	// })
	// if err := produer.SendMsg("play-event-qa", Message{
	// 	MessageBody:  []byte(`{"a":1,"b":false}`),
	// 	DelaySeconds: 0,
	// 	Priority:     8,
	// }); err != nil {
	// 	t.Error(err.Error())
	// }
}
