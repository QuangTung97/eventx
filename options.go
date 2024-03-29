package eventx

import (
	"log"
	"time"

	"go.uber.org/zap"
)

type eventxOptions struct {
	getLastEventsLimit         uint64
	getUnprocessedEventsLimit  uint64
	dbProcessorRetryTimer      time.Duration
	dbProcessorErrorRetryTimer time.Duration
	coreStoredEventsSize       uint64
	logger                     *zap.Logger
	errorLogger                func(err error)
}

// Option for configuration
type Option func(opts *eventxOptions)

func defaultErrorLogger(_ error) {
}

func defaultOptions() eventxOptions {
	return eventxOptions{
		getLastEventsLimit:         256,
		getUnprocessedEventsLimit:  256,
		dbProcessorRetryTimer:      60 * time.Second,
		dbProcessorErrorRetryTimer: 60 * time.Second,
		coreStoredEventsSize:       1024,
		logger:                     zap.NewNop(),
		errorLogger:                defaultErrorLogger,
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

// WithLogger configures error zap logger
func WithLogger(logger *zap.Logger) Option {
	return func(opts *eventxOptions) {
		opts.logger = logger
	}
}

// WithErrorLogger configures callback func for errors
func WithErrorLogger(fn func(err error)) Option {
	return func(opts *eventxOptions) {
		opts.errorLogger = fn
	}
}

// =================================================================
// Subscriber Options
// =================================================================

type subscriberOptions struct {
	sizeLimit uint64
}

// SubscriberOption for customizing subscribers
type SubscriberOption func(opts *subscriberOptions)

func computeSubscriberOptions(opts ...SubscriberOption) subscriberOptions {
	result := subscriberOptions{
		sizeLimit: 2 << 20, // 2MB
	}
	for _, o := range opts {
		o(&result)
	}
	return result
}

// WithSubscriberSizeLimit configures limit in size of Fetch batches
func WithSubscriberSizeLimit(sizeLimit uint64) SubscriberOption {
	return func(opts *subscriberOptions) {
		opts.sizeLimit = sizeLimit
	}
}

type retentionOptions struct {
	maxTotalEvents     uint64
	deleteBatchSize    uint64
	fetchLimit         uint64
	errorLogger        func(err error)
	errorRetryDuration time.Duration
}

// =================================================================
// Retention Options
// =================================================================

// RetentionOption ...
type RetentionOption func(opts *retentionOptions)

func defaultRetentionErrorLogger(err error) {
	log.Println("[ERROR] retention job:", err)
}

func computeRetentionOptions(options ...RetentionOption) retentionOptions {
	opts := retentionOptions{
		maxTotalEvents:     100000000, // 100,000,000
		deleteBatchSize:    256,
		fetchLimit:         1024,
		errorLogger:        defaultRetentionErrorLogger,
		errorRetryDuration: 30 * time.Second,
	}
	for _, o := range options {
		o(&opts)
	}
	return opts
}

// WithMaxTotalEvents keep the number of events not more than *maxSize*
func WithMaxTotalEvents(maxSize uint64) RetentionOption {
	return func(opts *retentionOptions) {
		opts.maxTotalEvents = maxSize
	}
}

// WithDeleteBatchSize specifies number events to be deleted with DeleteEventsBefore() method
func WithDeleteBatchSize(size uint64) RetentionOption {
	return func(opts *retentionOptions) {
		opts.deleteBatchSize = size
	}
}

// WithRetentionErrorLogger config the error logger
func WithRetentionErrorLogger(logger func(err error)) RetentionOption {
	return func(opts *retentionOptions) {
		opts.errorLogger = logger
	}
}

// WithRetentionErrorRetryDuration config the retry duration
func WithRetentionErrorRetryDuration(d time.Duration) RetentionOption {
	return func(opts *retentionOptions) {
		opts.errorRetryDuration = d
	}
}
