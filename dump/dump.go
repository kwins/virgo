package dump

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/juju/errors"
	"github.com/kwins/virgo/dump/config"
	"github.com/kwins/virgo/log"
)

// Unlick mysqldump, Dumper is designed for parsing and syning data easily.
type Dumper struct {
	cfg config.Config

	IgnoreTables map[string][]string

	// 错误输出
	ErrOut io.Writer

	masterDataSkipped bool
	maxAllowedPacket  int
	hexBlob           bool
}

func NewDumper(cfg config.Config) (*Dumper, error) {
	if len(cfg.Dump.MysqldumpPath) == 0 {
		cfg.Dump.MysqldumpPath = "/usr/local/mysql/bin/mysqldump"
	}
	log.Infof("use %s to dump", cfg.Dump.MysqldumpPath)

	_, err := exec.LookPath(cfg.Dump.MysqldumpPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	d := new(Dumper)
	d.cfg = cfg
	d.IgnoreTables = make(map[string][]string)
	d.masterDataSkipped = cfg.Dump.MasterDataSkipped
	d.ErrOut = os.Stderr
	return d, nil
}

func (d *Dumper) SetErrOut(o io.Writer) {
	d.ErrOut = o
}

// In some cloud MySQL, we have no privilege to use `--master-data`.
func (d *Dumper) SkipMasterData(v bool) {
	d.masterDataSkipped = v
}

func (d *Dumper) SetMaxAllowedPacket(i int) {
	d.maxAllowedPacket = i
}

func (d *Dumper) SetHexBlob(v bool) {
	d.hexBlob = v
}

func (d *Dumper) AddIgnoreTables(db string, tables ...string) {
	t, _ := d.IgnoreTables[db]
	t = append(t, tables...)
	d.IgnoreTables[db] = t
}

func (d *Dumper) Dump(w io.Writer) error {
	args := make([]string, 0, 16)

	// Common args
	seps := strings.Split(d.cfg.Mysql.Address, ":")
	args = append(args, fmt.Sprintf("--host=%s", seps[0]))
	if len(seps) > 1 {
		args = append(args, fmt.Sprintf("--port=%s", seps[1]))
	}

	args = append(args, fmt.Sprintf("--user=%s", d.cfg.Mysql.User))
	args = append(args, fmt.Sprintf("--password=%s", d.cfg.Mysql.Password))

	if !d.masterDataSkipped {
		args = append(args, "--master-data")
	}

	if d.maxAllowedPacket > 0 {
		// mysqldump param should be --max-allowed-packet=%dM not be --max_allowed_packet=%dM
		args = append(args, fmt.Sprintf("--max-allowed-packet=%dM", d.maxAllowedPacket))
	}

	args = append(args, "--single-transaction")
	args = append(args, "--skip-lock-tables")

	// Disable uncessary data
	args = append(args, "--compact")
	args = append(args, "--skip-opt")
	args = append(args, "--quick")

	// We only care about data
	args = append(args, "--no-create-info")

	// Multi row is easy for us to parse the data
	args = append(args, "--skip-extended-insert")

	if d.hexBlob {
		// Use hex for the binary type
		args = append(args, "--hex-blob")
	}

	for db, tables := range d.IgnoreTables {
		for _, table := range tables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", db, table))
		}
	}

	if len(d.cfg.Mysql.Charset) != 0 {
		args = append(args, fmt.Sprintf("--default-character-set=%s", d.cfg.Mysql.Charset))
	}

	if len(d.cfg.Dump.Where) != 0 {
		args = append(args, fmt.Sprintf("--where=%s", d.cfg.Dump.Where))
	}

	if len(d.cfg.Dump.Tables) == 0 && len(d.cfg.Dump.Databases) == 0 {
		args = append(args, "--all-databases")
	} else if len(d.cfg.Dump.Tables) == 0 {
		args = append(args, "--databases")
		args = append(args, d.cfg.Dump.Databases...)
	} else {
		args = append(args, d.cfg.Dump.TableDB)
		args = append(args, d.cfg.Dump.Tables...)

		// If we only dump some tables, the dump data will not have database name
		// which makes us hard to parse, so here we add it manually.

		w.Write([]byte(fmt.Sprintf("USE `%s`;\n", d.cfg.Dump.TableDB)))
	}

	log.Infof("exec mysqldump with %v", args)
	cmd := exec.Command(d.cfg.Dump.MysqldumpPath, args...)

	cmd.Stderr = d.ErrOut
	cmd.Stdout = w

	return cmd.Run()
}

// Dump MySQL and parse immediately
func (d *Dumper) DumpAndParse(h ParseHandler) error {
	r, w := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := Parse(r, h, !d.masterDataSkipped)
		r.CloseWithError(err)
		done <- err
	}()

	err := d.Dump(w)
	w.CloseWithError(err)

	err = <-done

	return errors.Trace(err)
}
