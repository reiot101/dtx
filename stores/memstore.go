package stores

import (
	"fmt"

	"github.com/patrickmn/go-cache"
	"github.com/reiot777/dtx/packet"
)

var _ Store = (*memStore)(nil)

type memStore struct {
	db *cache.Cache
}

func NewMemStore() Store {
	return &memStore{
		db: cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

// Append tx log to store
func (s *memStore) Append(log *packet.TxLog) error {
	var logs []*packet.TxLog

	if v, ok := s.db.Get(log.Id); ok {
		logs = v.([]*packet.TxLog)
	}

	logs = append(logs, log)
	s.db.Set(log.Id, logs, cache.NoExpiration)
	return nil
}

// Load tx logs by id
func (s *memStore) Load(id string) ([]*packet.TxLog, error) {
	var logs []*packet.TxLog

	if v, ok := s.db.Get(id); ok {
		logs = v.([]*packet.TxLog)
	}

	return logs, nil
}

// Keys return tx ids
func (s *memStore) Keys() ([]string, error) {
	items := s.db.Items()
	keys := make([]string, 0, len(items))

	for k := range items {
		keys = append(keys, k)
	}

	return keys, nil
}

// Flush tx logs by id
func (s *memStore) Flush(id string) error {
	s.db.Delete(id)
	return nil
}

// Last return last tx log by id
func (s *memStore) Last(id string) (*packet.TxLog, error) {
	var logs []*packet.TxLog

	if v, ok := s.db.Get(id); ok {
		logs = v.([]*packet.TxLog)
	}

	if n := len(logs); n > 0 {
		return logs[n-1], nil
	}

	return nil, fmt.Errorf("no such data")
}

// Close close connection
func (s *memStore) Close() error {
	s.db.Flush()
	return nil
}
