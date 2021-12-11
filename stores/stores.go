package stores

import "github.com/reiot777/dtx/packet"

type Store interface {
	// Append tx log to store
	Append(*packet.TxLog) error
	// Load tx logs by id
	Load(string) ([]*packet.TxLog, error)
	// Keys return tx ids
	Keys() ([]string, error)
	// Flush tx logs by id
	Flush(id string) error
	// Last return last tx log by id
	Last(id string) (*packet.TxLog, error)
	// Close close connection
	Close() error
}
