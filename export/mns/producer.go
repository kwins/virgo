package mns

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/kwins/virgo/config"
)

const (
	mnsVersion = "2015-06-06"
)

// Producer producer
type Producer struct {
	c   *http.Client
	cre Credential
	cfg config.MnsCfg
}

// NewProducer new mns producer
func NewProducer(cfg config.MnsCfg) *Producer {
	p := new(Producer)
	p.cfg = cfg
	p.cre = NewCredential(cfg.AccessSecret)

	// set request timeout
	transport := &http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(network, addr, time.Second*5)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		ResponseHeaderTimeout: time.Second * 35,
	}

	p.c = &http.Client{
		Transport: transport,
	}
	return p
}

// Send send mns request
func (producer *Producer) Send(
	method string, h http.Header,
	message interface{}, resource string) (*http.Response, error) {
	var err error
	var body []byte

	if message == nil {
		body = nil
	} else {
		switch m := message.(type) {
		case []byte:
			body = m
		default:
			if body, err = xml.Marshal(message); err != nil {
				return nil, err
			}
		}
	}

	bodyMd5 := md5.Sum(body)
	bodyMd5Str := fmt.Sprintf("%x", bodyMd5)
	if h == nil {
		h = make(http.Header)
	}

	h.Add("x-mns-version", mnsVersion)
	h.Add("Content-Type", "application/xml")
	h.Add("Content-MD5", base64.StdEncoding.EncodeToString([]byte(bodyMd5Str)))
	h.Add("Date", time.Now().UTC().Format(http.TimeFormat))

	signStr, err := producer.cre.Sign(h, method, resource)
	if err != nil {
		return nil, err
	}

	authSignStr := fmt.Sprintf("MNS %s:%s", producer.cfg.AccessID, signStr)
	h.Add("Authorization", authSignStr)
	url := producer.cfg.Host + "/" + resource
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header = h
	return producer.c.Do(req)
}

// SendMsg 发送消息
func (producer *Producer) SendMsg(qName string, message Message) error {
	resource := fmt.Sprintf("queues/%s/%s", qName, "messages")
	if message.Priority == 0 {
		message.Priority = 8
	}
	resp, err := producer.Send("POST", nil, &message, resource)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return toErr(resp)
}

// SendMsgs 批量发送消息,返回error代表不成功，error为错误信息
func (producer *Producer) SendMsgs(qName string, message ...Message) error {
	resource := fmt.Sprintf("queues/%s/%s", qName, "messages")

	var batchMsg BatchMessage
	batchMsg.Messages = make([]Message, len(message))
	for i, v := range message {
		if batchMsg.Messages[i].Priority == 0 {
			batchMsg.Messages[i].Priority = 8
		}
		batchMsg.Messages[i] = v
	}
	resp, err := producer.Send("POST", nil, &batchMsg, resource)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return toErr(resp)
}

func toErr(resp *http.Response) error {
	if resp.StatusCode != http.StatusCreated { // 创建失败
		decoder := xml.NewDecoder(resp.Body)
		var errMsg ErrorMessage
		if err := decoder.Decode(&errMsg); err != nil {
			return err
		}
		return errMsg.Error()
	}
	return nil
}
