package master

import (
	"bytes"
	"context"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/clientv3"
	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/mysql"
)

// Disk 使用磁盘文件存储mysql master 信息
type Etcd struct {
	pos mysql.Position
	c   *clientv3.Client
}

func getEtcdStore(c *clientv3.Client) *Etcd {
	var etcd Etcd
	etcd.c = c
	gResp, err := c.Get(context.TODO(), "/mysql/master/info")
	if err != nil {
		log.Error(err.Error())
		return &etcd
	}
	if len(gResp.Kvs) == 0 {
		return &etcd
	}
	kv := gResp.Kvs[0]

	var pos mysql.Position
	toml.DecodeReader(bytes.NewBuffer(kv.Value), &pos)
	etcd.pos = pos
	log.Infof("canald pull mysql position from etcd and initialize it with journal_name=%s pos=%d", pos.Name, pos.Pos)
	return &etcd
}

// Save 存储位置信息
func (etcd *Etcd) Save(pos mysql.Position) {
	pos.UT = time.Now().Unix()
	etcd.pos = pos
}

// Value 获取位置信息
func (etcd *Etcd) Value() mysql.Position {
	return etcd.pos
}

// Flush master 信息落地
func (etcd *Etcd) Flush() error {
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	if err := e.Encode(etcd.Value()); err != nil {
		return err
	}
	_, err := etcd.c.Put(context.TODO(), "/mysql/master/info", string(buf.Bytes()))
	if err != nil {
		return err
	}
	return nil
}
