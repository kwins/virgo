package canal

import (
	"context"
	"fmt"
	"time"

	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/canal/master"
)

// EventStore 事件存储
type EventStore struct {
	ctx    context.Context
	hasPut bool
	// event store 数组总长度
	size int64

	// 客户端设置的每次Get的Event大小
	batchSize int64

	// canald client 最后一个ACK的位点
	// canald client 最后一个get的位点
	// canald 从mysql同步后put到store的最后一个位点
	lastAckCursor int64
	lastGetCursor int64
	lastPutCursor int64

	// 存储RowEvent事件，队列
	entries []core.Entity

	// 负责mysql位置维护
	master *master.StoreManager
}

// NewEventStore new event store
func NewEventStore(size, batchSize int64,
	storer master.Store, flush time.Duration,
	ctx context.Context) *EventStore {

	store := new(EventStore)
	store.size = size
	store.entries = make([]core.Entity, store.size)
	store.batchSize = batchSize
	store.ctx = ctx
	store.master = master.NewStoreManager(storer, flush, ctx)
	return store
}

// Get 获取Rows Event,返回 size <= batchSize,没数据则返回 0 个
func (store *EventStore) Get() (int64, []core.Entity) {
	// log.Infof("<GET START> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d entries length=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor, len(store.entries))
	// 没有新的事件了
	if store.lastGetCursor == store.lastPutCursor {
		// log.Infof("<GET END> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d entries length=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor, len(store.entries))
		return -1, []core.Entity{}
	}

	getCursor, putCursor := store.lastGetCursor, store.lastPutCursor
	var entries = make([]core.Entity, 0, store.batchSize)
	var newLastGetCursor int64

	// GET < PUT 位置，在 0 的同一边
	if getCursor < putCursor {
		if getCursor+store.batchSize < putCursor { // 并且剩余为获取的事件 大于 batchSize
			newLastGetCursor = getCursor + store.batchSize
			entries = store.entries[getCursor+1 : newLastGetCursor+1]
		} else { // 并且 剩余未获取的事件 小于等于 batchSize
			newLastGetCursor = putCursor
			entries = store.entries[getCursor+1 : putCursor+1]
		}
		store.lastGetCursor = newLastGetCursor
		return newLastGetCursor, entries
	}

	// GET > PUT 位置，在 0 的两边

	// 并且 getCursor 在 0 的左边,且左边未获取的事件 大于等于 batchSize
	if left := store.size - getCursor - 1; left >= store.batchSize {
		newLastGetCursor = getCursor + store.batchSize
		store.lastGetCursor = newLastGetCursor
		entries = store.entries[getCursor+1 : newLastGetCursor+1]
		return newLastGetCursor, entries
	}

	// 并且 getCursor 在 0 的左边,且左边未获取的事件 小于 batchSize
	// 先获取左边剩余的 事件
	entries = append(entries, store.entries[getCursor+1:store.size]...)

	// 再获取右边的事件
	// 批量剩余未获取 小于 0的右边剩余的未获取的事件
	if batchLave := store.batchSize - int64(len(entries)); batchLave <= putCursor+1 {
		newLastGetCursor = batchLave - 1
		store.lastGetCursor = newLastGetCursor
		entries = append(entries, store.entries[:batchLave]...)
	} else {
		newLastGetCursor = putCursor
		store.lastGetCursor = newLastGetCursor
		entries = append(entries, store.entries[:putCursor+1]...)
	}
	// log.Infof("<GET END> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d entris length=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor, cap(store.entries))
	return newLastGetCursor, entries
}

// Ack 确认完成
func (store *EventStore) Ack(batchId int64) error {
	// log.Infof("<ACK START> barchID=%d lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d", batchId, store.lastAckCursor, store.lastGetCursor, store.lastPutCursor)
	if batchId != store.lastGetCursor {
		return fmt.Errorf("batchId %d must be last get cursor %d", batchId, store.lastGetCursor)
	}

	if store.lastAckCursor < batchId {
		// 完整的一段
		se := store.entries[store.lastAckCursor+1 : batchId+1]
		for l := len(se) - 1; l >= 0; l-- {
			if se[l].SavePos() {
				store.master.S.Save(se[l].Pos())
				// log.Infof("<ACK ING> save i=%d pos %s", l, se[l].Pos().String())
				break
			}
		}

	} else if store.lastAckCursor > batchId {

		// 被分成了两段
		se1 := store.entries[store.lastAckCursor+1:]
		for l := len(se1) - 1; l >= 0; l-- {
			if se1[l].SavePos() {
				// 找到倒数第一个可以保存的位点，保存它
				store.master.S.Save(se1[l].Pos())
				// log.Infof("<ACK ING> save i=%d pos %s", l, se1[l].Pos().String())
				break
			}
		}

		se2 := store.entries[:batchId]
		for l := len(se2) - 1; l >= 0; l-- {
			if se2[l].SavePos() {
				store.master.S.Save(se2[l].Pos())
				// log.Infof("<ACK ING> save i=%d pos %s", l, se2[l].Pos().String())
				break
			}
		}
	}

	store.lastAckCursor = batchId
	// log.Infof("<ACK END> barchID=%d lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d", batchId, store.lastAckCursor, store.lastGetCursor, store.lastPutCursor)
	return nil
}

// Rollback Get游标会退到ack游标
func (store *EventStore) Rollback() {
	store.lastGetCursor = store.lastAckCursor
	// log.Infof("<ROLLBACK> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor)
}

// Put put event row
func (store *EventStore) Put(rowEvent core.Entity) {
	// log.Infof("<PUT START> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor)
	var nextPutCursor int64
	// 第一次PUT
	if !store.hasPut && store.lastPutCursor == 0 {
		nextPutCursor = 0
	} else if store.lastPutCursor+1 < store.size {
		nextPutCursor = store.lastPutCursor + 1
	} else if store.lastPutCursor+1 == store.size {
		nextPutCursor = 0
	}

	if store.hasPut && nextPutCursor == store.lastAckCursor {
		// 阻塞检查直到可以写入为止
		ticker := time.NewTicker(time.Millisecond * 200)
		for {
			select {
			case <-ticker.C:
				if nextPutCursor != store.lastAckCursor {
					store.putEvent(nextPutCursor, rowEvent)
					return
				}
				// log.Infof("<PUT WAIT> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor)
			case <-store.ctx.Done():
				return
			}
		}
	}

	store.putEvent(nextPutCursor, rowEvent)
	// log.Infof("<PUT END> lastAckCursor=%d lastGetCursor=%d lastPutCursor=%d", store.lastAckCursor, store.lastGetCursor, store.lastPutCursor)
}

func (store *EventStore) putEvent(cursor int64, entity core.Entity) {
	store.entries[cursor] = entity
	store.lastPutCursor = cursor
	store.hasPut = true
}
