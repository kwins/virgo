package instance

// eNode 选举节点
type eNode struct {
	CanaldID uint16 `json:"canald_id"`
	Address  string `json:"address"`
}
