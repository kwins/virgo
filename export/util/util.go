package util

import (
	"fmt"
)

// Topic 组装Topic Name
func Topic(namespace, schema, tableName string) string {
	return fmt.Sprintf("%s-%s-%s", namespace, schema, tableName)
}
