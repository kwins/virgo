package export

import (
	"github.com/kwins/virgo/canal/core"
)

// 定义导出对象
const (
	CONSOLE = "console"
	ELASTIC = "elastic"
	KAFKA   = "kafka"
	NSQ     = "nsq"
	MNS     = "mns"
)

// Bulk 数据
type Bulk interface {
	INSERT(*core.RowsEvent) error
	UPDATE(*core.RowsEvent) error
	DELETE(*core.RowsEvent) error
	// 打印到控制台
	CONSOLE(*core.RowsEvent)
	CLOSE() error
}
