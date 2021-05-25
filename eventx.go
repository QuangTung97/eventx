package eventx

import (
	"context"
	"time"
)

// Event represents events
// type of *Data* is string instead of []byte for immutability
type Event struct {
	ID        uint64
	Seq       uint64
	Data      string
	CreatedAt time.Time
}

//go:generate moq -out eventx_mocks_test.go . Repository Timer

// Repository for accessing database
type Repository interface {
	GetLastEvents(ctx context.Context, limit uint64) ([]Event, error)
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]Event, error)

	UpdateSequences(ctx context.Context, events []Event) error
}

// Timer for timer
type Timer interface {
	Reset()
	Chan() <-chan time.Time
}
