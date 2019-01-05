package console

import (
	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/log"
)

// Export export data
type Export struct {
}

// NewExport new export
func NewExport() *Export {
	return &Export{}
}

// INSERT insert data to elastic
func (export *Export) INSERT(ev *core.RowsEvent) error {
	log.Info(ev.String())
	return nil
}

// UPDATE update data to elastic
func (export *Export) UPDATE(ev *core.RowsEvent) error {
	log.Info(ev.String())
	return nil
}

// DELETE delete data to elastic
func (export *Export) DELETE(ev *core.RowsEvent) error {
	log.Info(ev.String())
	return nil
}

// CONSOLE print data to console
func (export *Export) CONSOLE(ev *core.RowsEvent) {
	log.Info(ev.String())
}

// CLOSE close
func (export *Export) CLOSE() error {
	return nil
}
