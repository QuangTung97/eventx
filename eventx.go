package eventx

import (
	"context"
	"database/sql"
	"errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

// ErrEventNotFound when select from events table not find events >= sequence (because of retention)
var ErrEventNotFound = errors.New("not found any events from a sequence")

// EventConstraint a type constraint for event
type EventConstraint interface {
	// GetSequence returns the event sequence number, = 0 if sequence is null
	GetSequence() uint64

	// GetSize returns the approximate size (in bytes) of the event, for limit batch size by event data size
	// using WithSubscriberSizeLimit for configuring this limit
	GetSize() uint64
}

//go:generate moq -skip-ensure -out eventx_mocks_test.go . Repository RetentionRepository Timer

// Repository for accessing database, MUST be thread safe
type Repository[E EventConstraint] interface {
	// GetLastEvents returns top *limit* events (events with the highest sequence numbers),
	// by sequence number in ascending order, ignore events with null sequence number
	GetLastEvents(ctx context.Context, limit uint64) ([]E, error)

	// GetUnprocessedEvents returns list of events with the smallest event *id* (not sequence number)
	// *AND* have NULL sequence numbers, in ascending order of event *id*
	// size of the list is limited by *limit*
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]E, error)

	// GetEventsFrom returns list of events with sequence number >= *from*
	// in ascending order of event sequence numbers, ignoring events with null sequence numbers
	// size of the list is limited by *limit*
	GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]E, error)

	// UpdateSequences updates only sequence numbers of *events*
	UpdateSequences(ctx context.Context, events []E) error
}

// RetentionRepository for delete old events
type RetentionRepository interface {
	// GetMinSequence returns the min sequence number of all events (except events with null sequence numbers)
	// returns null if no events with sequence number existed
	GetMinSequence(ctx context.Context) (sql.NullInt64, error)

	// DeleteEventsBefore deletes events with sequence number < *beforeSeq*
	DeleteEventsBefore(ctx context.Context, beforeSeq uint64) error
}

// Timer for timer
type Timer interface {
	Reset()
	ResetAfterChan()
	Chan() <-chan time.Time
}

// Runner for running event handling
type Runner[E EventConstraint] struct {
	repo    Repository[E]
	options eventxOptions

	processor *dbProcessor[E]
	core      *coreService[E]
}

// Subscriber for subscribing to events
type Subscriber[E EventConstraint] struct {
	from           uint64
	fetchLimit     uint64
	fetchSizeLimit uint64

	repo *sizeLimitedRepo[E]

	core        *coreService[E]
	respChan    chan fetchResponse[E]
	placeholder []E
	receiving   bool
}

// NewRunner creates a Runner
func NewRunner[E EventConstraint](
	repo Repository[E], setSequence func(event *E, seq uint64),
	options ...Option,
) *Runner[E] {
	opts := computeOptions(options...)
	coreChan := make(chan []E, 256)
	processor := newDBProcessor(repo, coreChan, setSequence, opts)
	core := newCoreService(coreChan, opts)

	return &Runner[E]{
		repo:    repo,
		options: opts,

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

//revive:disable:cognitive-complexity
func (r *Runner[E]) runDBProcessor(ctx context.Context) {
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
			err = r.processor.runProcessor(ctx)
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

//revive:enable:cognitive-complexity

func (r *Runner[E]) runCoreService(ctx context.Context) {
	for {
		r.core.runCore(ctx)
		if ctx.Err() != nil {
			return
		}
	}
}

// Run the runner
func (r *Runner[E]) Run(ctx context.Context) {
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
func (r *Runner[E]) Signal() {
	r.processor.doSignal()
}

// NewSubscriber creates a subscriber
func (r *Runner[E]) NewSubscriber(from uint64, fetchLimit uint64, options ...SubscriberOption) *Subscriber[E] {
	opts := computeSubscriberOptions(options...)

	return &Subscriber[E]{
		from:           from,
		fetchLimit:     fetchLimit,
		fetchSizeLimit: opts.sizeLimit,

		repo: newSizeLimitedRepo[E](r.repo, fetchLimit, opts.sizeLimit),

		core:        r.core,
		respChan:    make(chan fetchResponse[E], 1),
		placeholder: make([]E, 0, fetchLimit),
	}
}

func cloneAndClearEvents[E any](events []E) []E {
	result := make([]E, len(events))
	copy(result, events)
	for i := range events {
		var empty E
		events[i] = empty
	}
	return result
}

// Fetch get events, if ctx is cancelled / deadline exceed then the fetch will be returned with error = nil,
// and then it can be call again with a normal context object
func (s *Subscriber[E]) Fetch(ctx context.Context) ([]E, error) {
	if !s.receiving {
		s.core.doFetch(fetchRequest[E]{
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
			if events[0].GetSequence() != s.from {
				return nil, ErrEventNotFound
			}
			s.from = events[len(events)-1].GetSequence() + 1

			unmarshalled := make([]E, 0, len(events))
			for _, e := range events {
				unmarshalled = append(unmarshalled, e)
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
