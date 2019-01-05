package instance

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

// Guard  负责 virgo 选主
type Guard struct {
	ctx context.Context
	c   *clientv3.Client
}

// NewGuard new gurad
func NewGuard(c *clientv3.Client, ctx context.Context) *Guard {
	var guard Guard
	guard.ctx = ctx
	guard.c = c
	return &guard
}

// Try 尝试运行当前服务实例
func (guard *Guard) Try(self eNode) error {
	session, err := concurrency.NewSession(guard.c,
		concurrency.WithContext(guard.ctx), concurrency.WithTTL(5))
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-session.Done():
				session.Close()
			case <-guard.ctx.Done():
				session.Close()
				return
			}
		}
	}()

	electe := concurrency.NewElection(session, "/virgo")
	val, err := json.Marshal(&self)
	if err != nil {
		return err
	}
	return electe.Campaign(guard.ctx, string(val))
}
