package elastic

import (
	"context"
	"fmt"
	"time"

	"github.com/kwins/virgo/canal/core"
	"github.com/kwins/virgo/config"
	"github.com/kwins/virgo/log"
	"github.com/olivere/elastic"
)

// Export export data
type Export struct {
	cfg config.ElasticSearchCfg
	api *elastic.Client
}

// NewExport new export
func NewExport(cfg config.ElasticSearchCfg) *Export {
	exp := new(Export)
	exp.cfg = cfg
	var ops []elastic.ClientOptionFunc
	ops = append(ops, elastic.SetURL(cfg.Hosts...), elastic.SetSniff(false))
	if cfg.Debug {
		l := log.GetDefaultLogger()
		ops = append(ops, elastic.SetMaxRetries(10), elastic.SetInfoLog(l),
			elastic.SetErrorLog(l), elastic.SetTraceLog(l))
	}
	log.Info(cfg)
	if cfg.UserName != "" || cfg.Password != "" {
		ops = append(ops, elastic.SetBasicAuth(cfg.UserName, cfg.Password))
	}
	var err error
	exp.api, err = elastic.NewClient(ops...)
	if err != nil {
		panic(err.Error())
	}
	return exp
}

// INSERT insert data to elastic
func (export *Export) INSERT(ev *core.RowsEvent) error {
	return export.Do(ev)
}

// UPDATE update data to elastic
func (export *Export) UPDATE(ev *core.RowsEvent) error {
	return export.Do(ev)
}

// DELETE delete data to elastic
func (export *Export) DELETE(ev *core.RowsEvent) error {
	return export.Do(ev)
}

// CONSOLE print data to console
func (export *Export) CONSOLE(ev *core.RowsEvent) {
	log.Info(ev.String())
}

// CLOSE close
func (export *Export) CLOSE() error {
	export.api.Stop()
	return nil
}

// Do do bulk
func (export *Export) Do(ev *core.RowsEvent) error {
	index := buildIndex(export.cfg.Namespace, ev.Table.Schema, ev.Table.Name)
	for _, v := range ev.Records(export.cfg.Namespace) {
		switch v.Action {
		case core.ACTION_DELETE:
			id := buildid(v.Columns[0], v.PkNames)
			_, err := export.api.Delete().Index(index).Type(ev.Table.Name).Id(id).Do(context.Background())
			if err != nil {
				log.Error(err.Error())
			}
		case core.ACTION_UPDATE:
			id := buildid(v.Columns[1], v.PkNames)
			var val = v.Columns[0]
			val["timestamp"] = time.Now().Add(-time.Hour * 8).Format("2006-01-02T15:04:05.000Z")
			_, err := export.api.Index().Index(index).Type(ev.Table.Name).Id(id).BodyJson(&val).Do(context.Background())
			if err != nil {
				return err
			}
		case core.ACTION_INSERT:
			id := buildid(v.Columns[0], v.PkNames)
			var val = v.Columns[0]
			val["timestamp"] = time.Now().Add(-time.Hour * 8).Format("2006-01-02T15:04:05.000Z")
			_, err := export.api.Index().Index(index).Type(ev.Table.Name).Id(id).BodyJson(&val).Do(context.Background())
			if err != nil {
				return err
			}
		}
		if export.cfg.Debug {
			log.Info(v.String())
		}
	}
	return nil
}

func buildIndex(namespace, schema, tabelName string) string {
	return fmt.Sprintf("%s.%s.%s", namespace, schema, tabelName)
}

func buildid(val map[string]interface{}, pknames []string) string {
	var id string
	var sep = ""
	for _, name := range pknames {
		id += fmt.Sprintf("%s%v", sep, val[name])
		sep = ":"
	}
	return id
}
