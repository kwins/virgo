package mysql

import (
	"fmt"
)

// For binlog filename + position based replication
type Position struct {
	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`
	UT   int64  `toml:"bin_update_time"`
}

func (p Position) Compare(o Position) int {
	// First compare binlog name
	if p.Name > o.Name {
		return 1
	} else if p.Name < o.Name {
		return -1
	} else {
		// Same binlog file, compare position
		if p.Pos > o.Pos {
			return 1
		} else if p.Pos < o.Pos {
			return -1
		} else {
			return 0
		}
	}
}

func (p Position) String() string {
	return fmt.Sprintf("(%s, %d)", p.Name, p.Pos)
}
