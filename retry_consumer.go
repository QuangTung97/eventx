package eventx

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RetryConsumer ...
type RetryConsumer[E EventConstraint] struct {
	conf retryConsumerConfig

	runner      *Runner[E]
	repo        Repository[E]
	setSequence SetSequenceFunc
	getSequence GetSequenceFunc
	handler     func(ctx context.Context, events []E) error
}

// GetSequenceFunc ...
type GetSequenceFunc func(ctx context.Context) (sql.NullInt64, error)

// SetSequenceFunc ...
type SetSequenceFunc func(ctx context.Context, seq uint64) error

// NewRetryConsumer ...
func NewRetryConsumer[E EventConstraint](
	runner *Runner[E],
	repo Repository[E],
	getSequence GetSequenceFunc,
	setSequence SetSequenceFunc,
	handler func(ctx context.Context, events []E) error,
	options ...RetryConsumerOption,
) *RetryConsumer[E] {
	conf := newRetryConsumerConfig(options)

	return &RetryConsumer[E]{
		conf:        conf,
		runner:      runner,
		repo:        repo,
		getSequence: getSequence,
		setSequence: setSequence,
		handler:     handler,
	}
}

func (c *RetryConsumer[E]) getLastSequence(ctx context.Context) (uint64, error) {
	nullLastSeq, err := c.getSequence(ctx)
	if err != nil {
		return 0, fmt.Errorf("retry consumer: get sequence: %w", err)
	}

	if nullLastSeq.Valid {
		return uint64(nullLastSeq.Int64), nil
	}

	lastEvents, err := c.repo.GetLastEvents(ctx, 1)
	if err != nil {
		return 0, fmt.Errorf("retry consumer: repo.GetLastEvents: %w", err)
	}

	if len(lastEvents) == 0 {
		err := c.setSequence(ctx, 0)
		if err != nil {
			return 0, fmt.Errorf("retry consumer: set sequence: %w", err)
		}
		return uint64(0), nil
	}

	lastSeq := lastEvents[len(lastEvents)-1].GetSequence()
	err = c.setSequence(ctx, lastSeq)
	if err != nil {
		return 0, fmt.Errorf("retry consumer: set sequence: %w", err)
	}
	return lastSeq, nil
}

func (c *RetryConsumer[E]) tryGetLastSequence(ctx context.Context) uint64 {
	for {
		lastSeq, err := c.getLastSequence(ctx)
		if ctx.Err() != nil {
			return 0
		}
		if err != nil {
			c.conf.errorLogger(err)
			sleepContext(ctx, c.conf.retryDuration)
			if ctx.Err() != nil {
				return 0
			}
			continue
		}
		return lastSeq
	}
}

// RunConsumer until the ctx is finished
//
//revive:disable-next-line:cognitive-complexity
func (c *RetryConsumer[E]) RunConsumer(ctx context.Context) {
	initSeq := c.tryGetLastSequence(ctx)
	if ctx.Err() != nil {
		return
	}

	sub := c.runner.NewSubscriber(initSeq+1, c.conf.fetchLimit)

	for {
		events, err := sub.Fetch(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			newErr := fmt.Errorf("retry consumer: subscriber fetch: %w", err)
			c.conf.errorLogger(newErr)

			sleepContext(ctx, c.conf.retryDuration)
			if ctx.Err() != nil {
				return
			}
			continue
		}

		lastSeq := events[len(events)-1].GetSequence()

		for {
			err := c.handler(ctx, events)
			if err != nil {
				newErr := fmt.Errorf("retry consumer: handler: %w", err)
				c.conf.errorLogger(newErr)

				sleepContext(ctx, c.conf.retryDuration)
				if ctx.Err() != nil {
					return
				}
				continue
			}
			break
		}

		for {
			err = c.setSequence(ctx, lastSeq)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				newErr := fmt.Errorf("retry consumer: set sequence: %w", err)
				c.conf.errorLogger(newErr)

				sleepContext(ctx, c.conf.retryDuration)
				if ctx.Err() != nil {
					return
				}
			}
			break
		}
	}
}

type retryConsumerConfig struct {
	retryDuration time.Duration
	errorLogger   func(err error)
	fetchLimit    uint64
}

func newRetryConsumerConfig(options []RetryConsumerOption) retryConsumerConfig {
	conf := retryConsumerConfig{
		retryDuration: 30 * time.Second,
		errorLogger: func(err error) {
			fmt.Println("[ERROR] Retry Consumer Error:", err)
		},
		fetchLimit: 16,
	}
	for _, fn := range options {
		fn(&conf)
	}
	return conf
}

// RetryConsumerOption ...
type RetryConsumerOption func(conf *retryConsumerConfig)

// WithConsumerRetryDuration ...
func WithConsumerRetryDuration(d time.Duration) RetryConsumerOption {
	return func(conf *retryConsumerConfig) {
		conf.retryDuration = d
	}
}

// WithRetryConsumerErrorLogger ...
func WithRetryConsumerErrorLogger(logger func(err error)) RetryConsumerOption {
	return func(conf *retryConsumerConfig) {
		conf.errorLogger = logger
	}
}
