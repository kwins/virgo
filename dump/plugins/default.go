package plugins

import (
	"bytes"

	"github.com/BurntSushi/toml"
	"github.com/kwins/virgo/canal/master"
	"github.com/kwins/virgo/dump/config"
	"github.com/kwins/virgo/export"
	"github.com/kwins/virgo/export/console"
	"github.com/kwins/virgo/export/elastic"
	"github.com/kwins/virgo/export/kafaka"
	"github.com/kwins/virgo/mysql"
)

// DefaultDumpHandler default handler
type DefaultDumpHandler struct {
	p         *parser
	masterDir string
	out       export.Bulk
}

// NewDefaultDumpHandler debug plugins
func NewDefaultDumpHandler(cfg config.Config) *DefaultDumpHandler {
	d := new(DefaultDumpHandler)
	d.p = newParser(cfg)
	d.masterDir = cfg.Dump.MasterDir
	switch cfg.Dump.Export {
	case export.CONSOLE:
		d.out = console.NewExport()
	case export.ELASTIC:
		d.out = elastic.NewExport(cfg.Elastic)
	case export.KAFKA:
		d.out = kafaka.NewExport(cfg.Kafka)
	}
	return d
}

// BinLog BinLog
func (dumpHandler *DefaultDumpHandler) BinLog(name string, pos uint64) error {
	dumpHandler.p.SavePos(name, pos)
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	if err := e.Encode(mysql.Position{
		Name: name,
		Pos:  uint32(pos),
	}); err != nil {
		return err
	}
	return master.WriteFileAtomic(dumpHandler.masterDir+"/master.info", buf.Bytes(), 0644)
}

func (dumpHandler *DefaultDumpHandler) Data(schema string, table string, values []string) error {
	event, err := dumpHandler.p.Parse(schema, table, values)
	if err != nil {
		return err
	}
	return dumpHandler.out.INSERT(event)
}
