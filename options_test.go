package eventx

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestComputeCoreOptions(t *testing.T) {
	table := []struct {
		name     string
		opts     eventxOptions
		expected eventxOptions
	}{
		{
			name: "default",
			opts: computeOptions(),
			expected: eventxOptions{
				getLastEventsLimit:        256,
				getUnprocessedEventsLimit: 256,
				dbProcessorRetryTimer:     60 * time.Second,
				coreStoredEventsSize:      1024,
			},
		},
		{
			name: "with-get-last-events-limit",
			opts: computeOptions(WithGetLastEventsLimit(50)),
			expected: eventxOptions{
				getLastEventsLimit:        50,
				getUnprocessedEventsLimit: 256,
				dbProcessorRetryTimer:     60 * time.Second,
				coreStoredEventsSize:      1024,
			},
		},
		{
			name: "with-get-unprocessed-events-limit",
			opts: computeOptions(WithGetUnprocessedEventsLimit(100)),
			expected: eventxOptions{
				getLastEventsLimit:        256,
				getUnprocessedEventsLimit: 100,
				dbProcessorRetryTimer:     60 * time.Second,
				coreStoredEventsSize:      1024,
			},
		},
		{
			name: "with-db-processor-retry-timer",
			opts: computeOptions(WithDBProcessorRetryTimer(20 * time.Second)),
			expected: eventxOptions{
				getLastEventsLimit:        256,
				getUnprocessedEventsLimit: 256,
				dbProcessorRetryTimer:     20 * time.Second,
				coreStoredEventsSize:      1024,
			},
		},
		{
			name: "with-core-stored-events-size",
			opts: computeOptions(WithCoreStoredEventsSize(2000)),
			expected: eventxOptions{
				getLastEventsLimit:        256,
				getUnprocessedEventsLimit: 256,
				dbProcessorRetryTimer:     60 * time.Second,
				coreStoredEventsSize:      2000,
			},
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, e.expected, e.opts)
		})
	}
}
