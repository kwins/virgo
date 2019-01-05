package master

import (
	"context"
	"time"

	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/mysql"
)

// Position msyql 同步 位置信息
type Store interface {
	Save(pos mysql.Position)
	Value() mysql.Position
	Flush() error
}

// StoreManager 管理mysql节点binlog信息
type StoreManager struct {
	S     Store
	flush time.Duration
	ctx   context.Context
}

// NewStoreManager new store position
func NewStoreManager(store Store, flush time.Duration, ctx context.Context) *StoreManager {
	pos := &StoreManager{
		S:     store,
		flush: flush,
		ctx:   ctx,
	}
	go pos.run()
	return pos
}

// run run with timer
func (storeManager *StoreManager) run() {
	ticker := time.NewTicker(storeManager.flush * time.Second)
	for {
		select {
		case <-storeManager.ctx.Done():
			if err := storeManager.S.Flush(); err != nil {
				log.Error(err.Error())
			}
			log.Infof("canald store manager stoped.")
			return
		case <-ticker.C:
			if err := storeManager.S.Flush(); err != nil {
				log.Error(err.Error())
			}
		}
	}
}
