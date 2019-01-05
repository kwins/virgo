package master

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/kwins/virgo/log"
	"github.com/kwins/virgo/mysql"
)

// Disk 使用磁盘文件存储mysql master 信息
type Disk struct {
	pos     mysql.Position
	dataDir string
}

// DiskStore new disk store
func getDiskStore(dataDir string) *Disk {

	pos := loadDiskStore(dataDir)
	log.Infof("canald initialize disk with journal_name=%s pos=%d datadir=%s", pos.Name, pos.Pos, dataDir)
	return &Disk{
		pos:     pos,
		dataDir: dataDir,
	}
}

func loadDiskStore(dataDir string) mysql.Position {
	var pos mysql.Position
	filePath := path.Join(dataDir, "master.info")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return pos
	}
	log.Infof("canald pull mysql position from file %s", filePath)
	f, err := os.Open(filePath)
	if err != nil && !os.IsNotExist(err) {
		return pos
	} else if os.IsNotExist(err) {
		return pos
	}
	defer f.Close()
	toml.DecodeReader(f, &pos)
	return pos
}

// Save 存储位置信息
func (disk *Disk) Save(pos mysql.Position) {
	pos.UT = time.Now().Unix()
	disk.pos = pos
}

// Value 获取位置信息
func (disk *Disk) Value() mysql.Position {
	return disk.pos
}

// Flush master 信息落地
func (disk *Disk) Flush() error {
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	if err := e.Encode(disk.Value()); err != nil {
		return err
	}
	if err := WriteFileAtomic(disk.dataDir+"/master.info", buf.Bytes(), 0644); err != nil {
		return err
	}
	return nil
}

func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Dir(filename), path.Base(filename)
	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), filename)
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}
