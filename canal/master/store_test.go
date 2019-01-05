package master

import (
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/kwins/virgo/mysql"
)

func TestDiskStore(t *testing.T) {
	disk := StoreBulder().DiskStore("/Users/quinn/goproj/laoyuegou.com/src/github.com/kwins/virgo/canal/master")
	disk.Save(mysql.Position{
		Name: "test-bin.0000001",
		Pos:  123,
		UT:   time.Now().Unix(),
	})
	if err := disk.Flush(); err != nil {
		t.Error(err.Error())
	}
}

func TestEtcdStore(t *testing.T) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
		Username:  "",
		Password:  "",
	})
	if err != nil {
		t.Error(err.Error())
		return
	}
	etcd := StoreBulder().EtcdStore(c)
	etcd.Save(mysql.Position{
		Name: "test-bin.0000002",
		Pos:  145,
	})
	if err := etcd.Flush(); err != nil {
		t.Error(err.Error())
	}
}
func TestReverse(t *testing.T) {
	var s = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for i, v := range s[2:7] {
		t.Log(i, "   ", v)
	}
}
