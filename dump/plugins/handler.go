package plugins

import (
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/dump/config"
	"github.com/kwins/virgo/export"
	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/schema"
)

var (
	ErrSkip = errors.New("Handler error, but skipped")
)

// ParseHandler parse dump data handler
type ParseHandler interface {
	// Parse CHANGE MASTER TO MASTER_LOG_FILE=name, MASTER_LOG_POS=pos;
	BinLog(name string, pos uint64) error

	Data(schema string, table string, values []string) error
}

type parser struct {
	name   string
	pos    uint64
	caches *Tables
	export export.Bulk
}

func newParser(cfg config.Config) *parser {
	h := new(parser)
	h.caches = NewTables(cfg.Mysql)
	return h
}

// SavePos 记录dump的数据的binlog位置
func (h *parser) SavePos(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	log.Infof("dump binlog name=%s pos=%d", name, pos)
	return nil
}

// Data 解析数据
func (h *parser) Parse(db string, table string, values []string) (*core.RowsEvent, error) {

	tableInfo, err := h.caches.GetTable(db, table)
	if err != nil {
		log.Errorf("get %s.%s information err: %v", db, table, err)
		return nil, errors.Trace(err)
	}

	vs := make([]interface{}, len(values))

	for i, v := range values {
		if v == "NULL" {
			vs[i] = nil
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					log.Errorf("parse row %v at %d error %v, skip", values, i, err)
					return nil, ErrSkip
				}
				vs[i] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					log.Errorf("parse row %v at %d error %v, skip", values, i, err)
					return nil, ErrSkip
				}
				vs[i] = f
			} else if strings.HasPrefix(v, "0x") {
				buf, err := hex.DecodeString(v[2:])
				if err != nil {
					log.Errorf("parse row %v at %d error %v, skip", values, i, err)
					return nil, ErrSkip
				}
				vs[i] = string(buf)
			} else {
				log.Errorf("parse row %v error, invalid type at %d, skip", values, i)
				return nil, ErrSkip
			}
		} else {
			vs[i] = v[1 : len(v)-1]
		}
	}

	// 这里 logName 参数不会使用，可以为空
	return core.NewRowsEvent(tableInfo, core.ACTION_INSERT, "", [][]interface{}{vs}, nil), nil
}
