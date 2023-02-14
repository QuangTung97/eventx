package eventx

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func drainRespChan(ch <-chan fetchResponse[testEvent]) fetchResponse[testEvent] {
	select {
	case resp := <-ch:
		return resp
	default:
		return fetchResponse[testEvent]{}
	}
}

type testEvent struct {
	id   uint64
	seq  uint64
	size uint64
}

func (e testEvent) GetSequence() uint64 {
	return e.seq
}

func (e testEvent) GetSize() uint64 {
	return e.size
}

func setTestEventSeq(event *testEvent, seq uint64) {
	event.seq = seq
}

func newCoreServiceTest(coreChan <-chan []testEvent, opts eventxOptions) *coreService[testEvent] {
	return newCoreService(coreChan, opts)
}

func TestCoreServiceFetch_SimpleCase(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []testEvent{
		{id: 10, seq: 20},
		{id: 8, seq: 21},
		{id: 12, seq: 22},
	}

	ctx := context.Background()
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       2,
		from:        20,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))

	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 10, seq: 20},
			{id: 8, seq: 21},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_From_Zero(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []testEvent{
		{id: 10, seq: 20},
		{id: 8, seq: 21},
		{id: 12, seq: 22},
	}

	ctx := context.Background()
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       2,
		from:        0,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))

	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: false,
	}, resp)
	assert.Equal(t, 0, cap(resp.events))
}

func TestCoreServiceRun_ContextCancel(t *testing.T) {
	t.Parallel()

	s := newCoreServiceTest(nil, computeOptions())

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	s.runCore(ctx)
}

func TestCoreServiceFetch_Behind(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []testEvent{
		{id: 8, seq: 30},
	}

	ctx := context.Background()
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       2,
		from:        20,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))

	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: false,
	}, resp)
}

func TestCoreServiceFetch_Multiple_Core_Events__Behind(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []testEvent{
		{id: 8, seq: 30},
	}
	coreChan <- []testEvent{
		{id: 11, seq: 31},
	}
	ctx := context.Background()
	s.runCore(ctx)
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       2,
		from:        30,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 8, seq: 30},
			{id: 11, seq: 31},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_Limit_Exceed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []testEvent{
		{id: 8, seq: 30},
	}
	coreChan <- []testEvent{
		{id: 11, seq: 31},
		{id: 15, seq: 32},
	}
	ctx := context.Background()
	s.runCore(ctx)
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       5,
		from:        30,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 8, seq: 30},
			{id: 11, seq: 31},
			{id: 15, seq: 32},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_SizeLimit_Exceed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []testEvent{
		{id: 8, seq: 30, size: 10},
	}
	coreChan <- []testEvent{
		{id: 11, seq: 31, size: 11},
		{id: 15, seq: 32, size: 12},
	}
	ctx := context.Background()
	s.runCore(ctx)
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		from:        30,
		limit:       5,
		sizeLimit:   32,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 8, seq: 30, size: 10},
			{id: 11, seq: 31, size: 11},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_Wrap_Around_And_Override_Not_Exist(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
	}
	coreChan <- []testEvent{
		{id: 11, seq: 32},
		{id: 15, seq: 33},
		{id: 18, seq: 34},
	}
	ctx := context.Background()
	s.runCore(ctx)
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       5,
		from:        30,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: false,
	}, resp)
}

func TestCoreServiceFetch_Wrap_Around_And_Override_Exist(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
	}
	coreChan <- []testEvent{
		{id: 11, seq: 32},
		{id: 15, seq: 33},
		{id: 18, seq: 34},
	}
	ctx := context.Background()
	s.runCore(ctx)
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       6,
		from:        31,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 9, seq: 31},
			{id: 11, seq: 32},
			{id: 15, seq: 33},
			{id: 18, seq: 34},
		},
	}, resp)
}

func TestCoreServiceFetch_Add_To_Wait_List(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
		{id: 11, seq: 32},
		{id: 15, seq: 33},
		{id: 18, seq: 34},
	}
	ctx := context.Background()
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       2,
		from:        35,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []testEvent{
		{id: 20, seq: 35},
	}
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 20, seq: 35},
		},
	}, resp)

	coreChan <- []testEvent{
		{id: 25, seq: 36},
	}
	s.runCore(ctx)
	assert.Equal(t, 0, len(respChan))
}

func TestCoreServiceFetch_Wait_To_The_Far_Future(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
		{id: 11, seq: 32},
		{id: 15, seq: 33},
		{id: 18, seq: 34},
	}
	ctx := context.Background()
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       2,
		from:        36,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []testEvent{
		{id: 20, seq: 35},
	}
	s.runCore(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []testEvent{
		{id: 22, seq: 36},
		{id: 23, seq: 37},
	}
	s.runCore(ctx)
	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 22, seq: 36},
			{id: 23, seq: 37},
		},
	}, resp)
}

func TestCoreServiceFetch_Core_Events_Not_In_Order__Not_Existed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
		{id: 11, seq: 32},
	}
	ctx := context.Background()
	s.runCore(ctx)

	coreChan <- []testEvent{
		{id: 14, seq: 38},
		{id: 12, seq: 39},
		{id: 18, seq: 40},
	}
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       3,
		from:        37,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)
	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse[testEvent]{
		existed: false,
	}, drainRespChan(respChan))
}

func TestCoreServiceFetch_Core_Events_Not_In_Order__Existed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
		{id: 11, seq: 32},
	}
	ctx := context.Background()
	s.runCore(ctx)

	coreChan <- []testEvent{
		{id: 14, seq: 38},
		{id: 12, seq: 39},
		{id: 18, seq: 40},
	}
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       3,
		from:        38,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)
	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 14, seq: 38},
			{id: 12, seq: 39},
			{id: 18, seq: 40},
		},
	}, drainRespChan(respChan))
}

func TestCoreServiceFetch_Wait_And_Events_Not_In_Order(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
		{id: 11, seq: 32},
	}
	ctx := context.Background()
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       3,
		from:        34,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []testEvent{
		{id: 14, seq: 38},
		{id: 12, seq: 39},
		{id: 18, seq: 40},
	}
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse[testEvent]{
		existed: false,
	}, drainRespChan(respChan))
}

func TestCoreServiceFetch_Core_Events_Go_Backward(t *testing.T) {
	t.Parallel()

	coreChan := make(chan []testEvent, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []testEvent{
		{id: 8, seq: 30},
		{id: 9, seq: 31},
		{id: 11, seq: 32},
	}
	ctx := context.Background()
	s.runCore(ctx)

	coreChan <- []testEvent{
		{id: 14, seq: 20},
		{id: 12, seq: 21},
		{id: 18, seq: 22},
	}
	s.runCore(ctx)

	respChan := make(chan fetchResponse[testEvent], 1)
	s.doFetch(fetchRequest[testEvent]{
		limit:       6,
		from:        20,
		placeholder: make([]testEvent, 0, 100),
		respChan:    respChan,
	})
	s.runCore(ctx)

	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse[testEvent]{
		existed: true,
		events: []testEvent{
			{id: 14, seq: 20},
			{id: 12, seq: 21},
			{id: 18, seq: 22},
		},
	}, drainRespChan(respChan))
}

func TestComputeRequestToResponse(t *testing.T) {
	table := []struct {
		name   string
		req    fetchRequest[testEvent]
		first  uint64
		last   uint64
		stored []testEvent
		resp   fetchResponse[testEvent]
	}{
		{
			name: "normal",
			req: fetchRequest[testEvent]{
				from:  10,
				limit: 4,
			},
			stored: []testEvent{
				{id: 12, seq: 12, size: 1},
				{id: 13, seq: 13, size: 1},
				{id: 10, seq: 10, size: 1},
				{id: 11, seq: 11, size: 1},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10, size: 1},
					{id: 11, seq: 11, size: 1},
					{id: 12, seq: 12, size: 1},
					{id: 13, seq: 13, size: 1},
				},
			},
		},
		{
			name: "limit-below-last",
			req: fetchRequest[testEvent]{
				from:  10,
				limit: 3,
			},
			stored: []testEvent{
				{id: 12, seq: 12},
				{id: 13, seq: 13},
				{id: 10, seq: 10},
				{id: 11, seq: 11},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10},
					{id: 11, seq: 11},
					{id: 12, seq: 12},
				},
			},
		},
		{
			name: "limit-above-last",
			req: fetchRequest[testEvent]{
				from:  10,
				limit: 5,
			},
			stored: []testEvent{
				{id: 12, seq: 12},
				{id: 13, seq: 13},
				{id: 10, seq: 10},
				{id: 11, seq: 11},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10},
					{id: 11, seq: 11},
					{id: 12, seq: 12},
					{id: 13, seq: 13},
				},
			},
		},
		{
			name: "non-existed",
			req: fetchRequest[testEvent]{
				from:  9,
				limit: 3,
			},
			stored: []testEvent{
				{id: 12, seq: 12},
				{id: 13, seq: 13},
				{id: 10, seq: 10},
				{id: 11, seq: 11},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: false,
			},
		},
		{
			name: "size-limit-accept-two-events",
			req: fetchRequest[testEvent]{
				from:      10,
				limit:     3,
				sizeLimit: 10,
			},
			stored: []testEvent{
				{id: 12, seq: 12, size: 1},
				{id: 13, seq: 13, size: 2},
				{id: 10, seq: 10, size: 3},
				{id: 11, seq: 11, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10, size: 3},
					{id: 11, seq: 11, size: 7},
				},
			},
		},
		{
			name: "size-limit-smaller-than-first-event",
			req: fetchRequest[testEvent]{
				from:      10,
				limit:     3,
				sizeLimit: 4,
			},
			stored: []testEvent{
				{id: 12, seq: 12, size: 1},
				{id: 13, seq: 13, size: 2},
				{id: 10, seq: 10, size: 5},
				{id: 11, seq: 11, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10, size: 5},
				},
			},
		},
		{
			name: "size-limit-just-equal-first-event",
			req: fetchRequest[testEvent]{
				from:      10,
				limit:     3,
				sizeLimit: 5,
			},
			stored: []testEvent{
				{id: 12, seq: 12, size: 1},
				{id: 13, seq: 13, size: 2},
				{id: 10, seq: 10, size: 5},
				{id: 11, seq: 11, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10, size: 5},
				},
			},
		},
		{
			name: "size-limit-two-events",
			req: fetchRequest[testEvent]{
				from:      10,
				limit:     3,
				sizeLimit: 19,
			},
			stored: []testEvent{
				{id: 12, seq: 12, size: 8},
				{id: 13, seq: 13, size: 2},
				{id: 10, seq: 10, size: 5},
				{id: 11, seq: 11, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10, size: 5},
					{id: 11, seq: 11, size: 7},
				},
			},
		},
		{
			name: "size-limit-three-events",
			req: fetchRequest[testEvent]{
				from:      10,
				limit:     3,
				sizeLimit: 20,
			},
			stored: []testEvent{
				{id: 12, seq: 12, size: 8},
				{id: 13, seq: 13, size: 2},
				{id: 10, seq: 10, size: 5},
				{id: 11, seq: 11, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse[testEvent]{
				existed: true,
				events: []testEvent{
					{id: 10, seq: 10, size: 5},
					{id: 11, seq: 11, size: 7},
					{id: 12, seq: 12, size: 8},
				},
			},
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			resp := computeRequestToResponse(e.req, e.first, e.last, e.stored)
			assert.Equal(t, e.resp, resp)
		})
	}
}

func TestWaitListRemoveIf(t *testing.T) {
	table := []struct {
		name     string
		waitList []fetchRequest[testEvent]
		last     uint64
		offset   int
		expected []fetchRequest[testEvent]
	}{
		{
			name: "no-remove",
			waitList: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   8,
			offset: 3,
			expected: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
		},
		{
			name: "single-remove",
			waitList: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   9,
			offset: 2,
			expected: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 3, from: 9},
				{limit: 2, from: 8},
			},
		},
		{
			name: "two-remove",
			waitList: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   10,
			offset: 1,
			expected: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 3, from: 9},
				{limit: 2, from: 8},
			},
		},
		{
			name: "all-remove",
			waitList: []fetchRequest[testEvent]{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   11,
			offset: 0,
			expected: []fetchRequest[testEvent]{
				{limit: 2, from: 8},
				{limit: 3, from: 9},
				{limit: 1, from: 10},
			},
		},
		{
			name: "normal",
			waitList: []fetchRequest[testEvent]{
				{limit: 0, from: 7},
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
				{limit: 4, from: 11},
				{limit: 5, from: 10},
			},
			last:   10,
			offset: 3,
			expected: []fetchRequest[testEvent]{
				{limit: 5, from: 10},
				{limit: 1, from: 10},
				{limit: 4, from: 11},
				{limit: 3, from: 9},
				{limit: 2, from: 8},
				{limit: 0, from: 7},
			},
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()
			offset := waitListRemoveIf(e.waitList, func(req fetchRequest[testEvent]) bool {
				return req.from < e.last
			})
			assert.Equal(t, e.offset, offset)
			assert.Equal(t, e.expected, e.waitList)
		})
	}
}
