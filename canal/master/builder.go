package master

import (
	"github.com/coreos/etcd/clientv3"
)

// StoreBulder store builder
func StoreBulder() *Builder {
	return &Builder{}
}

// Builder builder build posotion store
type Builder struct {
}

// EtcdStore save positon use etcd
func (build *Builder) EtcdStore(c *clientv3.Client) *Etcd {
	return getEtcdStore(c)
}

// DiskStore save positon use disk
func (build *Builder) DiskStore(dataDir string) *Disk {
	return getDiskStore(dataDir)
}
