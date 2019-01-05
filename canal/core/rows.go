package core

import (
	"fmt"

	"github.com/kwins/virgo/mysql"
	"github.com/kwins/virgo/replication"
	"github.com/kwins/virgo/schema"
)

// The action name for sync.
const (
	ACTION_UPDATE = "update"
	ACTION_INSERT = "insert"
	ACTION_DELETE = "delete"
)

// Message binlog消息
type Message struct {
	entris  []Entity
	batchID int64
}

// NewMessage new message
func NewMessage(entris []Entity, bid int64) *Message {
	return &Message{
		entris:  entris,
		batchID: bid,
	}
}

// BatchID batchID
func (message *Message) BatchID() int64 {
	return message.batchID
}

// Reset reset
func (message *Message) Reset(batid int64, entitys []Entity) {
	message.batchID = batid
	message.entris = entitys
}

// Events all message events
func (message *Message) Events() []Entity {
	return message.entris
}

// Len length
func (message *Message) Len() int {
	return len(message.entris)
}

// Entity 事件实体
// 实体会直接存储在EventStore中
// 不同类型的Event实体，则处理不同
// RowEvent实体不需要保存位置
// XIDEvent，RotateEvent需要保存位置
// 保存位置信息时机为，Canald ACK后，倒叙拿到实体列表的最近的一个可保存的实体，获取位置信息缓存并落地
// 落地的策略根据配置可进行文件和Etcd落地
type Entity interface {
	SavePos() bool       // 实体ACK后是否需要保存位置
	Pos() mysql.Position // 获取位置信息
	String() string
}

// XIDEvent 事件结束
type XIDEvent struct {
	Save    bool
	LogPos  uint32
	LogName string
}

// SavePos 实体ACK后是否需要保存位置
func (r *XIDEvent) SavePos() bool {
	return r.Save
}

// Pos 获取位置信息
func (r *XIDEvent) Pos() mysql.Position {
	return mysql.Position{
		Name: r.LogName,
		Pos:  r.LogPos,
	}
}

// String string
func (r *XIDEvent) String() string {
	return "{Save:" + fmt.Sprint(r.Save) +
		" LogPos:" + fmt.Sprint(r.LogPos) +
		" LogName:" + fmt.Sprint(r.LogName) + "}"
}

// RotateEvent binglog文件轮转
type RotateEvent struct {
	Save    bool
	LogPos  uint32
	LogName string
}

// SavePos 实体ACK后是否需要保存位置
func (r *RotateEvent) SavePos() bool {
	return r.Save
}

// Pos 获取位置信息
func (r *RotateEvent) Pos() mysql.Position {
	return mysql.Position{
		Name: r.LogName,
		Pos:  r.LogPos,
	}
}

// String string
func (r *RotateEvent) String() string {
	return "{Save:" + fmt.Sprint(r.Save) +
		" LogPos:" + fmt.Sprint(r.LogPos) +
		" LogName:" + fmt.Sprint(r.LogName) + "}"
}

// RowsEvent is the event for row replication.
type RowsEvent struct {
	LogName string // binlog 文件名称
	Save    bool   // 是否保存位点
	Table   *schema.Table
	Action  string
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Rows [][]interface{}
	// Header can be used to inspect the event
	Header *replication.EventHeader
}

// NewRowsEvent new rows event
func NewRowsEvent(table *schema.Table, action, logName string,
	rows [][]interface{}, header *replication.EventHeader) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = rows
	e.Header = header
	e.LogName = logName
	e.Save = false
	e.handleUnsignedAndBytes()

	return e
}

func (r *RowsEvent) handleUnsignedAndBytes() {
	// Handle Unsigned Columns here, for binlog replication, we can't know the integer is unsigned or not,
	// so we use int type but this may cause overflow outside sometimes, so we must convert to the really .
	// unsigned type
	// if len(r.Table.UnsignedColumns) == 0 {
	// 	return
	// }

	for i := 0; i < len(r.Rows); i++ {
		for _, index := range r.Table.UnsignedColumns {
			switch t := r.Rows[i][index].(type) {
			case int8:
				r.Rows[i][index] = uint8(t)
			case int16:
				r.Rows[i][index] = uint16(t)
			case int32:
				r.Rows[i][index] = uint32(t)
			case int64:
				r.Rows[i][index] = uint64(t)
			case int:
				r.Rows[i][index] = uint(t)
			default:
			}
		}

		// byte,bytes 转为string，防止json编码问题
		for j := 0; j < len(r.Rows[i]); j++ {
			switch v := r.Rows[i][j].(type) {
			case []byte:
				r.Rows[i][j] = string(v)
				r.Table.Columns[j].Type = schema.TYPE_STRING
			case byte:
				r.Rows[i][j] = int8(v)
				r.Table.Columns[j].Type = schema.TYPE_NUMBER
			case nil: // null
				column := r.Table.Columns[j]
				switch column.Type {
				case schema.TYPE_NUMBER: // tinyint, smallint, mediumint, int, bigint, year
					r.Rows[i][j] = 0
				case schema.TYPE_FLOAT: // float, double
					r.Rows[i][j] = 0.00
				case schema.TYPE_ENUM: // enum
					r.Rows[i][j] = ""
				case schema.TYPE_SET: // set
					r.Rows[i][j] = ""
				case schema.TYPE_STRING: // other
					r.Rows[i][j] = ""
				case schema.TYPE_DATETIME: // datetime
					r.Rows[i][j] = ""
				case schema.TYPE_TIMESTAMP: // timestamp
					r.Rows[i][j] = ""
				case schema.TYPE_DATE: // date
					r.Rows[i][j] = ""
				case schema.TYPE_TIME: // time
					r.Rows[i][j] = ""
				case schema.TYPE_BIT: // bit
					r.Rows[i][j] = ""
				case schema.TYPE_JSON: // json
					r.Rows[i][j] = ""
				default:
				}
			}
		}
	}
}

// SavePos 实体ACK后是否需要保存位置
func (r *RowsEvent) SavePos() bool {
	return r.Save
}

// Pos 获取位置信息
func (r *RowsEvent) Pos() mysql.Position {
	return mysql.Position{
		Name: r.LogName,
		Pos:  r.Header.LogPos,
	}
}

// String implements fmt.Stringer interface.
func (r *RowsEvent) String() string {
	s := fmt.Sprintf("*****************************%s %s*****************************\n\n", r.Action, r.Table.String())
	switch r.Action {
	case ACTION_UPDATE:
		s += splice(r.Rows, "更新的数据")

	case ACTION_INSERT:
		s += splice(r.Rows, "插入的数据")

	case ACTION_DELETE:
		s += splice(r.Rows, "删除的数据")

	default:
		s += splice(r.Rows, "未知的数据")
	}

	var columns string
	for _, v := range r.Table.Columns {
		columns += fmt.Sprintf("Name:%20s SetValues:%30v Type:%d Collation:%20s EnumValues:%30v IsAuto:%t IsUnsigned:%t\n",
			v.Name, v.SetValues, v.Type, v.Collation, v.EnumValues, v.IsAuto, v.IsUnsigned)
	}

	s += "\n"
	s += fmt.Sprintf("数据库字段:\n%-s\n", columns)
	s += fmt.Sprintf("数据库主键:%-v\n", r.Table.PKColumns)
	return s
}

func splice(rows [][]interface{}, message string) string {
	var s string
	for i := range rows {
		var ss = make([]string, len(rows[i]))
		for j, v := range rows[i] {
			switch ev := v.(type) {
			case int, int8, int16, int32, int64:
				ss[j] = fmt.Sprint(ev)
			case string:
				ss[j] = ev
			case []byte:
				ss[j] = string(ev)
			default:
				ss[j] = fmt.Sprint(ev)
			}
		}
		s += fmt.Sprintf("%s(%d):%-v\n", message, len(rows), ss)
	}
	return s
}
