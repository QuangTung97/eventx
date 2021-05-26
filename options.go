package eventx

import (
	"go.uber.org/zap"
	"time"
)

type eventxOptions struct {
	getLastEventsLimit         uint64
	getUnprocessedEventsLimit  uint64
	dbProcessorRetryTimer      time.Duration
	dbProcessorErrorRetryTimer time.Duration
	coreStoredEventsSize       uint64
	logger                     *zap.Logger
}

// Option for configuration
type Option func(opts *eventxOptions)

func defaultOptions() eventxOptions {
	return eventxOptions{
		getLastEventsLimit:         256,
		getUnprocessedEventsLimit:  256,
		dbProcessorRetryTimer:      60 * time.Second,
		dbProcessorErrorRetryTimer: 60 * time.Second,
		coreStoredEventsSize:       1024,
		logger:                     zap.NewNop(),
	}
}

func computeOptions(options ...Option) eventxOptions {
	opts := defaultOptions()
	for _, o := range options {
		o(&opts)
	}
	return opts
}

// WithGetLastEventsLimit configures GetLastEvents limit
func WithGetLastEventsLimit(limit uint64) Option {
	return func(opts *eventxOptions) {
		opts.getLastEventsLimit = limit
	}
}

// WithGetUnprocessedEventsLimit configures GetUnprocessedEvents limit
func WithGetUnprocessedEventsLimit(limit uint64) Option {
	return func(opts *eventxOptions) {
		opts.getUnprocessedEventsLimit = limit
	}
}

// WithDBProcessorRetryTimer configures retry timer duration
func WithDBProcessorRetryTimer(d time.Duration) Option {
	return func(opts *eventxOptions) {
		opts.dbProcessorRetryTimer = d
	}
}

// WithDBProcessorErrorRetryTimer configures retry timer duration
func WithDBProcessorErrorRetryTimer(d time.Duration) Option {
	return func(opts *eventxOptions) {
		opts.dbProcessorErrorRetryTimer = d
	}
}

// WithCoreStoredEventsSize configures the size of stored events
func WithCoreStoredEventsSize(size uint64) Option {
	return func(opts *eventxOptions) {
		opts.coreStoredEventsSize = size
	}
}

// WithLogger configures error logger
func WithLogger(logger *zap.Logger) Option {
	return func(opts *eventxOptions) {
		opts.logger = logger
	}
}
