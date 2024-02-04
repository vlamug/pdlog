package server

import (
	"errors"
	"sync"
)

var ErrOffsetNotFound = errors.New("offset not found")

// Log contains log data
type Log struct {
	mu      sync.Mutex
	records []*Record
}

// NewLog creates new log
func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record *Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (*Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return nil, ErrOffsetNotFound
	}

	return c.records[offset], nil
}

// Record contains log item
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

func NewRecord(offset uint64, value []byte) *Record {
	return &Record{Offset: offset, Value: value}
}
