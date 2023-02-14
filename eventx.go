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

// EventConstraint a type constraint for event
type EventConstraint interface {
	GetSequence() uint64
	GetSize() uint64
}

//go:generate moq -skip-ensure -out eventx_mocks_test.go . Repository Timer

// Repository for accessing database
type Repository[E EventConstraint] interface {
	GetLastEvents(ctx context.Context, limit uint64) ([]E, error)
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]E, error)
	GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]E, error)

	UpdateSequences(ctx context.Context, events []E) error
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

// Fetch get events, if ctx is cancelled / deadline exceed then the fetch will returned with error = nil
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
