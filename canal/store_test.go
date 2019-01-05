package canal

import (
	"fmt"
	"testing"

	"github.com/juju/errors"
)

func TestEventStoreGet(t *testing.T) {

}

func TestShift(t *testing.T) {
	t.Log(2 << 16)
}

func TestErrors(t *testing.T) {
	t.Log(errors.Annotatef(fmt.Errorf("aaaaaa"), "table id %d", 123))
}
