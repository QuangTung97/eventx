package eventx

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestComputeCoreOptions(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	table := []struct {
		name     string
		opts     eventxOptions
		expected func(opts *eventxOptions)
	}{
		{
			name:     "default",
			opts:     computeOptions(),
			expected: func(opts *eventxOptions) {},
		},
		{
			name: "with-get-last-events-limit",
			opts: computeOptions(WithGetLastEventsLimit(50)),
			expected: func(opts *eventxOptions) {
				opts.getLastEventsLimit = 50
			},
		},
		{
			name: "with-get-unprocessed-events-limit",
			opts: computeOptions(WithGetUnprocessedEventsLimit(100)),

			expected: func(opts *eventxOptions) {
				opts.getUnprocessedEventsLimit = 100
			},
		},
		{
			name: "with-db-processor-retry-timer",
			opts: computeOptions(WithDBProcessorRetryTimer(20 * time.Second)),
			expected: func(opts *eventxOptions) {
				opts.dbProcessorRetryTimer = 20 * time.Second
			},
		},
		{
			name: "with-core-stored-events-size",
			opts: computeOptions(WithCoreStoredEventsSize(2000)),
			expected: func(opts *eventxOptions) {
				opts.coreStoredEventsSize = 2000

			},
		},
		{
			name: "with-db-processor-error-retry-timer",
			opts: computeOptions(WithDBProcessorErrorRetryTimer(20 * time.Second)),
			expected: func(opts *eventxOptions) {
				opts.dbProcessorErrorRetryTimer = 20 * time.Second
			},
		},
		{
			name: "with-logger",
			opts: computeOptions(WithLogger(logger)),
			expected: func(opts *eventxOptions) {
				opts.logger = logger
			},
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			opts := defaultOptions()
			e.expected(&opts)
			assert.Equal(t, opts, e.opts)
		})
	}
}
