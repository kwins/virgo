package canal

import (
	"fmt"
	"regexp"
	"time"

	"github.com/juju/errors"
	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/mysql"
	"github.com/kwins/virgo/replication"
	"github.com/satori/go.uuid"
)

var regexps = []*regexp.Regexp{
	regexp.MustCompile("(?i)^CREATE\\sTABLE(\\sIF\\sNOT\\sEXISTS)?\\s`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s.*"),
	regexp.MustCompile("(?i)^ALTER\\sTABLE\\s.*?`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s.*"),
	regexp.MustCompile("(?i)^RENAME\\sTABLE\\s.*?`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}\\s{1,}TO\\s.*?"),
	regexp.MustCompile("(?i)^DROP\\sTABLE(\\sIF\\sEXISTS){0,1}\\s`{0,1}(.*?)`{0,1}\\.{0,1}`{0,1}([^`\\.]+?)`{0,1}(?:$|\\s)"),
}

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	pos := c.store.master.S.Value()
	s, err := c.syncer.StartSync(pos)
	if err != nil {
		return nil, errors.Errorf("start sync replication at binlog %v error %v", pos, err)
	}
	log.Infof("canal start success, sync binlog at binlog file %v", pos)
	return s, nil
}

func (c *Canal) processSyncBinlog(ev *replication.BinlogEvent) error {
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		c.curBinlogName = string(e.NextLogName)
		c.store.Put(&core.RotateEvent{
			LogName: string(e.NextLogName),
			LogPos:  uint32(e.Position),
			Save:    true,
		})
		// log.Infof("<RotateEvent>  name=%s pos=%d", c.curBinlogName, e.Position)

	case *replication.RowsEvent:
		rowEvent, err := c.assembeRowEvent(ev)
		if err != nil {
			if err == ErrExcludedTable {
				return nil
			}
			return err
		}
		c.store.Put(rowEvent)
		// log.Infof("<RowsEvent>  name=%s pos=%d", rowEvent.LogName, rowEvent.Header.LogPos)

	case *replication.XIDEvent:
		c.store.Put(&core.XIDEvent{
			Save:    true,
			LogName: c.curBinlogName,
			LogPos:  ev.Header.LogPos,
		})
		// log.Infof("<XIDEvent>  name=%s pos=%d", c.curBinlogName, ev.Header.LogPos)

	case *replication.MariadbGTIDEvent:
		// try to save the GTID later
		gtid, err := mysql.ParseMariadbGTIDSet(e.GTID.String())
		if err != nil {
			return errors.Trace(err)
		}
		if err := c.eventHandler.OnGTID(gtid); err != nil {
			return errors.Trace(err)
		}

	case *replication.GTIDEvent:
		u, _ := uuid.FromBytes(e.SID)
		gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
		if err != nil {
			return errors.Trace(err)
		}
		if err := c.eventHandler.OnGTID(gtid); err != nil {
			return errors.Trace(err)
		}

	case *replication.QueryEvent:
		var (
			mb     [][]byte
			schema []byte
			table  []byte
		)

		for _, reg := range regexps {
			mb = reg.FindSubmatch(e.Query)
			if len(mb) != 0 {
				break
			}
		}

		mbLen := len(mb)
		if mbLen == 0 {
			return nil
		}

		// the first last is table name, the second last is database name(if exists)
		if len(mb[mbLen-2]) == 0 {
			schema = e.Schema
		} else {
			schema = mb[mbLen-2]
		}
		table = mb[mbLen-1]

		c.ClearTableCache(schema, table)
		log.Infof("table structure changed, clear table cache: %s.%s\n", schema, table)
		if err := c.eventHandler.OnTableChanged(string(schema), string(table)); err != nil {
			return errors.Trace(err)
		}

		// Now we only handle Table Changed DDL, maybe we will support more later.
		if err := c.eventHandler.OnDDL(c.store.master.S.Value(), e); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *Canal) runSyncBinlog() {
	var err error
	c.streamer, err = c.startSyncer()
	if err != nil {
		log.Error(err.Error())
		return
	}

	for {
		select {
		case ev := <-c.streamer.Get():
			if err := c.processSyncBinlog(ev); err != nil {
				log.Error(err.Error())
			}
		case err := <-c.streamer.Err():
			log.Error(err.Error())
		case <-c.ctx.Done():
			log.Infof("canal run sync binlog stoped.")
			return
		}
	}
}

func (c *Canal) assembeRowEvent(e *replication.BinlogEvent) (*core.RowsEvent, error) {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTable(schema, table)
	if err != nil {
		return nil, err
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = core.ACTION_INSERT
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = core.ACTION_DELETE
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = core.ACTION_UPDATE
	default:
		return nil, errors.Errorf("%s not supported now", e.Header.EventType)
	}
	return core.NewRowsEvent(t, action, c.curBinlogName, ev.Rows, e.Header), nil
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	return errors.Trace(err)
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			err := c.FlushBinlog()
			if err != nil {
				return errors.Trace(err)
			}
			curPos := c.store.master.S.Value()
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				log.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (c *Canal) GetMasterPos() (mysql.Position, error) {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{Name: "", Pos: 0}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{
		Name: name,
		Pos:  uint32(pos),
	}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Canald.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Canald.Flavor, gx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	return c.WaitUntilPos(pos, timeout)
}
