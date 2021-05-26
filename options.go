package eventx

import "time"

type eventxOptions struct {
	getLastEventsLimit        uint64
	getUnprocessedEventsLimit uint64
	dbProcessorRetryTimer     time.Duration
}

// Option for configuration
type Option func(opts *eventxOptions)

func computeOptions(options ...Option) eventxOptions {
	opts := eventxOptions{
		getLastEventsLimit:        1024,
		getUnprocessedEventsLimit: 1024,
		dbProcessorRetryTimer:     60 * time.Second,
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
