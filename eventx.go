package eventx

import (
	"context"
	"time"
)

// Event represents events
// use type of *Data* is string instead of []byte for immutability
type Event struct {
	ID        uint64
	Seq       uint64
	Data      string
	CreatedAt time.Time
}

//go:generate moq -stub -out eventx_mocks.go . Repository

// Repository for accessing database
type Repository interface {
	GetLastEvents(ctx context.Context, limit uint64) ([]Event, error)
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]Event, error)

	UpdateSequences(ctx context.Context, events []Event) error
}
