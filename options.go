package eventx

import "time"

type eventxOptions struct {
	getLastEventsLimit        uint64
	getUnprocessedEventsLimit uint64
	dbProcessorRetryTimer     time.Duration
	coreStoredEventsSize      uint64
}

// Option for configuration
type Option func(opts *eventxOptions)

func computeOptions(options ...Option) eventxOptions {
	opts := eventxOptions{
		getLastEventsLimit:        256,
		getUnprocessedEventsLimit: 256,
		dbProcessorRetryTimer:     60 * time.Second,
		coreStoredEventsSize:      1024,
	}
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

// WithCoreStoredEventsSize configures the size of stored events
func WithCoreStoredEventsSize(size uint64) Option {
	return func(opts *eventxOptions) {
		opts.coreStoredEventsSize = size
	}
}
