package core

import (
	"encoding/json"
	"fmt"
)

// Record 数据库记录变更结构
type Record struct {
	// 区分多个用户空间
	Namespace string                   `json:"namespace"` // 命名空间
	Action    string                   `json:"action"`
	Schema    string                   `json:"schema"`
	Name      string                   `json:"name"`
	PkNames   []string                 `json:"pk_names"`
	Columns   []map[string]interface{} `json:"columns"` // 字段 和 对应的值
}

// String string
func (record Record) String() string {
	b, _ := json.Marshal(&record)
	return string(b)
}

// ColumnsBytes columns bytes
func (record Record) ColumnsBytes() ([]byte, error) {
	return json.Marshal(record.Columns)
}

// Index index
func (record Record) Index() string {
	return fmt.Sprintf("%s-%s-%s", record.Namespace, record.Schema, record.Name)
}

// Type type
func (record Record) Type() string {
	return record.Name
}

// Record 解析一个Record
func (r *RowsEvent) Records(namespace string) []Record {
	var records []Record
	switch r.Action {
	case ACTION_INSERT, ACTION_DELETE:
		records = make([]Record, len(r.Rows))
		for i, row := range r.Rows {
			var row2 = [][]interface{}{row}
			records[i] = r.record(row2)
			records[i].Namespace = namespace
		}
	case ACTION_UPDATE:
		var idx int
		records = make([]Record, len(r.Rows)/2)
		for i := 2; i <= len(r.Rows); i += 2 {
			row2 := r.Rows[i-2 : i]
			records[idx] = r.record(row2)
			records[idx].Namespace = namespace
			idx++
		}
	}
	return records
}

// row2 一行或者两行数据
func (r *RowsEvent) record(row2 [][]interface{}) Record {
	columns := r.Table.Columns
	var record Record
	record.Action = r.Action
	record.Schema = r.Table.Schema
	record.Name = r.Table.Name
	record.PkNames = make([]string, len(r.Table.PKColumns))
	for i, v := range r.Table.PKColumns {
		record.PkNames[i] = columns[v].Name
	}
	record.Columns = make([]map[string]interface{}, len(row2))
	for i, r1 := range row2 {
		meta := make(map[string]interface{})
		for j, r2 := range r1 {
			meta[columns[j].Name] = r2
		}
		record.Columns[i] = meta
	}
	return record
}
