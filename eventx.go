package eventx

import (
	"context"
	"go.uber.org/zap"
	"sync"
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
	GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]Event, error)

	UpdateSequences(ctx context.Context, events []Event) error
}

// Timer for timer
type Timer interface {
	Reset()
	ResetAfterChan()
	Chan() <-chan time.Time
}

// Runner for running event handling
type Runner struct {
	options   eventxOptions
	repo      Repository
	processor *dbProcessor
	core      *coreService
}

// Subscriber for subscribing to events
type Subscriber struct {
	from        uint64
	fetchLimit  uint64
	repo        Repository
	core        *coreService
	respChan    chan fetchResponse
	placeholder []UnmarshalledEvent
}

// NewRunner creates a Runner
func NewRunner(repo Repository, options ...Option) *Runner {
	opts := computeOptions(options...)
	coreChan := make(chan coreEvents, 256)
	processor := newDBProcessor(repo, coreChan, opts)
	core := newCoreService(coreChan, opts)

	return &Runner{
		options:   opts,
		repo:      repo,
		processor: processor,
		core:      core,
	}
}

func sleepContext(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (r *Runner) runDBProcessor(ctx context.Context) {
OuterLoop:
	for {
		err := r.processor.init(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			r.options.logger.Error("DB Processor Init Error", zap.Error(err))
			sleepContext(ctx, r.options.dbProcessorErrorRetryTimer)
			if ctx.Err() != nil {
				return
			}
			continue
		}

		for {
			err = r.processor.run(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				r.options.logger.Error("DB Processor Run Error", zap.Error(err))
				sleepContext(ctx, r.options.dbProcessorErrorRetryTimer)
				if ctx.Err() != nil {
					return
				}
				continue OuterLoop
			}
		}
	}
}

func (r *Runner) runCoreService(ctx context.Context) {
	for {
		r.core.run(ctx)
		if ctx.Err() != nil {
			return
		}
	}
}

// Run starts the runner
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		r.runDBProcessor(ctx)
	}()

	go func() {
		defer wg.Done()
		r.runCoreService(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}

// Signal to db processor
func (r *Runner) Signal() {
	r.processor.signal()
}

// NewSubscriber creates a subscriber
func (r *Runner) NewSubscriber(from uint64, fetchLimit uint64) *Subscriber {
	return &Subscriber{
		from:        from,
		fetchLimit:  fetchLimit,
		repo:        r.repo,
		core:        r.core,
		respChan:    make(chan fetchResponse, 1),
		placeholder: make([]UnmarshalledEvent, 0, fetchLimit),
	}
}

func cloneAndClearEvents(events []UnmarshalledEvent) []UnmarshalledEvent {
	result := make([]UnmarshalledEvent, len(events))
	copy(result, events)
	for i := range events {
		events[i] = UnmarshalledEvent{}
	}
	return result
}

// Fetch get events
func (s *Subscriber) Fetch(ctx context.Context) ([]UnmarshalledEvent, error) {
	s.core.fetch(fetchRequest{
		from:        s.from,
		limit:       s.fetchLimit,
		placeholder: s.placeholder,
		respChan:    s.respChan,
	})

	select {
	case resp := <-s.respChan:
		if !resp.existed {
			events, err := s.repo.GetEventsFrom(ctx, s.from, s.fetchLimit)
			if err != nil {
				return nil, err
			}
			if len(events) > 0 {
				s.from = events[len(events)-1].Seq + 1
			}

			unmarshalled := make([]UnmarshalledEvent, 0, len(events))
			for _, e := range events {
				unmarshalled = append(unmarshalled, unmarshalEvent(e))
			}
			return unmarshalled, nil
		}
		s.from = resp.events[len(resp.events)-1].getSequence() + 1
		return cloneAndClearEvents(resp.events), nil

	case <-ctx.Done():
		return nil, nil
	}
}
