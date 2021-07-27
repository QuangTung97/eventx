package eventx

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

// ErrEventNotFound when select from events table not find any events
var ErrEventNotFound = errors.New("not found any events from a sequence")

// Event represents events
// type of *Data* is string instead of []byte for immutability
type Event struct {
	ID        uint64
	Seq       uint64
	Data      string
	CreatedAt time.Time
}

// UnmarshalledEvent is the output of UnmarshalEvent
type UnmarshalledEvent interface {
	// GetSequence is function for get sequence from UnmarshalledEvent
	GetSequence() uint64
}

// UnmarshalEvent is callback function for unmarshal binary to UnmarshalledEvent
type UnmarshalEvent func(e Event) UnmarshalledEvent

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
	repo      Repository
	unmarshal UnmarshalEvent
	options   eventxOptions

	processor *dbProcessor
	core      *coreService
}

// Subscriber for subscribing to events
type Subscriber struct {
	from           uint64
	fetchLimit     uint64
	fetchSizeLimit uint64

	repo      *sizeLimitedRepo
	unmarshal UnmarshalEvent

	core        *coreService
	respChan    chan fetchResponse
	placeholder []UnmarshalledEvent
	receiving   bool
}

// NewRunner creates a Runner
func NewRunner(repo Repository, unmarshal UnmarshalEvent, options ...Option) *Runner {
	opts := computeOptions(options...)
	coreChan := make(chan coreEvents, 256)
	processor := newDBProcessor(repo, coreChan, opts)
	core := newCoreService(coreChan, unmarshal, opts)

	return &Runner{
		repo:      repo,
		unmarshal: unmarshal,
		options:   opts,

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
func (r *Runner) NewSubscriber(from uint64, fetchLimit uint64, options ...SubscriberOption) *Subscriber {
	opts := computeSubscriberOptions(options...)

	return &Subscriber{
		from:           from,
		fetchLimit:     fetchLimit,
		fetchSizeLimit: opts.sizeLimit,

		repo:      newSizeLimitedRepo(r.repo, fetchLimit, opts.sizeLimit),
		unmarshal: r.unmarshal,

		core:        r.core,
		respChan:    make(chan fetchResponse, 1),
		placeholder: make([]UnmarshalledEvent, 0, fetchLimit),
	}
}

func cloneAndClearEvents(events []UnmarshalledEvent) []UnmarshalledEvent {
	result := make([]UnmarshalledEvent, len(events))
	copy(result, events)
	for i := range events {
		events[i] = nil
	}
	return result
}

// Fetch get events
func (s *Subscriber) Fetch(ctx context.Context) ([]UnmarshalledEvent, error) {
	if !s.receiving {
		s.core.fetch(fetchRequest{
			from:        s.from,
			limit:       s.fetchLimit,
			sizeLimit:   s.fetchSizeLimit,
			placeholder: s.placeholder,
			respChan:    s.respChan,
		})
		s.receiving = true
	}

	select {
	case resp := <-s.respChan:
		s.receiving = false

		if !resp.existed {
			events, err := s.repo.getEventsFrom(ctx, s.from)
			if err != nil {
				return nil, err
			}
			if len(events) == 0 {
				return nil, ErrEventNotFound
			}
			s.from = events[len(events)-1].Seq + 1

			unmarshalled := make([]UnmarshalledEvent, 0, len(events))
			for _, e := range events {
				unmarshalled = append(unmarshalled, s.unmarshal(e))
			}
			return unmarshalled, nil
		}
		s.from = resp.events[len(resp.events)-1].GetSequence() + 1
		return cloneAndClearEvents(resp.events), nil

	case <-ctx.Done():
		return nil, nil
	}
}

// MergeContext merge the contexts
func MergeContext(a, b context.Context) context.Context {
	mergeCtx, mergeCancel := context.WithCancel(a)

	go func() {
		select {
		case <-mergeCtx.Done(): // avoid goroutine leak
		case <-b.Done():
			mergeCancel()
		}
	}()

	return mergeCtx
}
