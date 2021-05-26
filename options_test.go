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
				getLastEventsLimit:        1024,
				getUnprocessedEventsLimit: 1024,
				dbProcessorRetryTimer:     60 * time.Second,
			},
		},
		{
			name: "with-get-last-events-limit",
			opts: computeOptions(WithGetLastEventsLimit(50)),
			expected: eventxOptions{
				getLastEventsLimit:        50,
				getUnprocessedEventsLimit: 1024,
				dbProcessorRetryTimer:     60 * time.Second,
			},
		},
		{
			name: "with-get-unprocessed-events-limit",
			opts: computeOptions(WithGetUnprocessedEventsLimit(100)),
			expected: eventxOptions{
				getLastEventsLimit:        1024,
				getUnprocessedEventsLimit: 100,
				dbProcessorRetryTimer:     60 * time.Second,
			},
		},
		{
			name: "with-db-processor-retry-timer",
			opts: computeOptions(WithDBProcessorRetryTimer(20 * time.Second)),
			expected: eventxOptions{
				getLastEventsLimit:        1024,
				getUnprocessedEventsLimit: 1024,
				dbProcessorRetryTimer:     20 * time.Second,
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
