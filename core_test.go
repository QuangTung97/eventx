package eventx

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func drainRespChan(ch <-chan fetchResponse) fetchResponse {
	select {
	case resp := <-ch:
		return resp
	default:
		return fetchResponse{}
	}
}

type testEvent struct {
	id  uint64
	seq uint64
}

func (e testEvent) GetSequence() uint64 {
	return e.seq
}

func unmarshalEvent(e Event) UnmarshalledEvent {
	return testEvent{
		id:  e.ID,
		seq: e.Seq,
	}
}

func newCoreServiceTest(coreChan <-chan coreEvents, opts eventxOptions) *coreService {
	return newCoreService(coreChan, unmarshalEvent, opts)
}

func TestCoreServiceFetch_SimpleCase(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []Event{
		{ID: 10, Seq: 20},
		{ID: 8, Seq: 21},
		{ID: 12, Seq: 22},
	}

	ctx := context.Background()
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       2,
		from:        20,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))

	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 10, Seq: 20}),
			unmarshalEvent(Event{ID: 8, Seq: 21}),
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceRun_ContextCancel(t *testing.T) {
	t.Parallel()

	s := newCoreServiceTest(nil, computeOptions())

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	s.run(ctx)
}

func TestCoreServiceFetch_Behind(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []Event{
		{ID: 8, Seq: 30},
	}

	ctx := context.Background()
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       2,
		from:        20,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))

	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: false,
	}, resp)
}

func TestCoreServiceFetch_Multiple_Core_Events__Behind(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []Event{
		{ID: 8, Seq: 30},
	}
	coreChan <- []Event{
		{ID: 11, Seq: 31},
	}
	ctx := context.Background()
	s.run(ctx)
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       2,
		from:        30,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 8, Seq: 30}),
			unmarshalEvent(Event{ID: 11, Seq: 31}),
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_Limit_Exceed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []Event{
		{ID: 8, Seq: 30},
	}
	coreChan <- []Event{
		{ID: 11, Seq: 31},
		{ID: 15, Seq: 32},
	}
	ctx := context.Background()
	s.run(ctx)
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       5,
		from:        30,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 8, Seq: 30}),
			unmarshalEvent(Event{ID: 11, Seq: 31}),
			unmarshalEvent(Event{ID: 15, Seq: 32}),
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func stringSize(n int) string {
	data := make([]byte, n)
	for i := range data {
		data[i] = 'A'
	}
	return string(data)
}

func TestCoreServiceFetch_SizeLimit_Exceed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions())

	coreChan <- []Event{
		{ID: 8, Seq: 30, Data: stringSize(10)},
	}
	coreChan <- []Event{
		{ID: 11, Seq: 31, Data: stringSize(11)},
		{ID: 15, Seq: 32, Data: stringSize(12)},
	}
	ctx := context.Background()
	s.run(ctx)
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		from:        30,
		limit:       5,
		sizeLimit:   32,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 8, Seq: 30}),
			unmarshalEvent(Event{ID: 11, Seq: 31}),
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_Wrap_Around_And_Override_Not_Exist(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
	}
	coreChan <- []Event{
		{ID: 11, Seq: 32},
		{ID: 15, Seq: 33},
		{ID: 18, Seq: 34},
	}
	ctx := context.Background()
	s.run(ctx)
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       5,
		from:        30,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: false,
	}, resp)
}

func TestCoreServiceFetch_Wrap_Around_And_Override_Exist(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
	}
	coreChan <- []Event{
		{ID: 11, Seq: 32},
		{ID: 15, Seq: 33},
		{ID: 18, Seq: 34},
	}
	ctx := context.Background()
	s.run(ctx)
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       6,
		from:        31,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 9, Seq: 31}),
			unmarshalEvent(Event{ID: 11, Seq: 32}),
			unmarshalEvent(Event{ID: 15, Seq: 33}),
			unmarshalEvent(Event{ID: 18, Seq: 34}),
		},
	}, resp)
}

func TestCoreServiceFetch_Add_To_Wait_List(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
		{ID: 11, Seq: 32},
		{ID: 15, Seq: 33},
		{ID: 18, Seq: 34},
	}
	ctx := context.Background()
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       2,
		from:        35,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []Event{
		{ID: 20, Seq: 35},
	}
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 20, Seq: 35}),
		},
	}, resp)

	coreChan <- []Event{
		{ID: 25, Seq: 36},
	}
	s.run(ctx)
	assert.Equal(t, 0, len(respChan))
}

func TestCoreServiceFetch_Wait_To_The_Far_Future(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
		{ID: 11, Seq: 32},
		{ID: 15, Seq: 33},
		{ID: 18, Seq: 34},
	}
	ctx := context.Background()
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       2,
		from:        36,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []Event{
		{ID: 20, Seq: 35},
	}
	s.run(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []Event{
		{ID: 22, Seq: 36},
		{ID: 23, Seq: 37},
	}
	s.run(ctx)
	assert.Equal(t, 1, len(respChan))
	resp := drainRespChan(respChan)
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 22, Seq: 36}),
			unmarshalEvent(Event{ID: 23, Seq: 37}),
		},
	}, resp)
}

func TestCoreServiceFetch_Core_Events_Not_In_Order__Not_Existed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
		{ID: 11, Seq: 32},
	}
	ctx := context.Background()
	s.run(ctx)

	coreChan <- []Event{
		{ID: 14, Seq: 38},
		{ID: 12, Seq: 39},
		{ID: 18, Seq: 40},
	}
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       3,
		from:        37,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)
	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse{
		existed: false,
	}, drainRespChan(respChan))
}

func TestCoreServiceFetch_Core_Events_Not_In_Order__Existed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
		{ID: 11, Seq: 32},
	}
	ctx := context.Background()
	s.run(ctx)

	coreChan <- []Event{
		{ID: 14, Seq: 38},
		{ID: 12, Seq: 39},
		{ID: 18, Seq: 40},
	}
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       3,
		from:        38,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)
	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 14, Seq: 38}),
			unmarshalEvent(Event{ID: 12, Seq: 39}),
			unmarshalEvent(Event{ID: 18, Seq: 40}),
		},
	}, drainRespChan(respChan))
}

func TestCoreServiceFetch_Wait_And_Events_Not_In_Order(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
		{ID: 11, Seq: 32},
	}
	ctx := context.Background()
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       3,
		from:        34,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 0, len(respChan))

	coreChan <- []Event{
		{ID: 14, Seq: 38},
		{ID: 12, Seq: 39},
		{ID: 18, Seq: 40},
	}
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse{
		existed: false,
	}, drainRespChan(respChan))
}

func TestCoreServiceFetch_Core_Events_Go_Backward(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreServiceTest(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

	coreChan <- []Event{
		{ID: 8, Seq: 30},
		{ID: 9, Seq: 31},
		{ID: 11, Seq: 32},
	}
	ctx := context.Background()
	s.run(ctx)

	coreChan <- []Event{
		{ID: 14, Seq: 20},
		{ID: 12, Seq: 21},
		{ID: 18, Seq: 22},
	}
	s.run(ctx)

	respChan := make(chan fetchResponse, 1)
	s.fetch(fetchRequest{
		limit:       6,
		from:        20,
		placeholder: make([]UnmarshalledEvent, 0, 100),
		respChan:    respChan,
	})
	s.run(ctx)

	assert.Equal(t, 1, len(respChan))
	assert.Equal(t, fetchResponse{
		existed: true,
		events: []UnmarshalledEvent{
			unmarshalEvent(Event{ID: 14, Seq: 20}),
			unmarshalEvent(Event{ID: 12, Seq: 21}),
			unmarshalEvent(Event{ID: 18, Seq: 22}),
		},
	}, drainRespChan(respChan))
}

func TestComputeRequestToResponse(t *testing.T) {
	table := []struct {
		name   string
		req    fetchRequest
		first  uint64
		last   uint64
		stored []storedEvent
		resp   fetchResponse
	}{
		{
			name: "normal",
			req: fetchRequest{
				from:  10,
				limit: 4,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}, size: 1},
				{event: testEvent{id: 13, seq: 13}, size: 1},
				{event: testEvent{id: 10, seq: 10}, size: 1},
				{event: testEvent{id: 11, seq: 11}, size: 1},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
					testEvent{id: 11, seq: 11},
					testEvent{id: 12, seq: 12},
					testEvent{id: 13, seq: 13},
				},
			},
		},
		{
			name: "limit-below-last",
			req: fetchRequest{
				from:  10,
				limit: 3,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}},
				{event: testEvent{id: 13, seq: 13}},
				{event: testEvent{id: 10, seq: 10}},
				{event: testEvent{id: 11, seq: 11}},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
					testEvent{id: 11, seq: 11},
					testEvent{id: 12, seq: 12},
				},
			},
		},
		{
			name: "limit-above-last",
			req: fetchRequest{
				from:  10,
				limit: 5,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}},
				{event: testEvent{id: 13, seq: 13}},
				{event: testEvent{id: 10, seq: 10}},
				{event: testEvent{id: 11, seq: 11}},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
					testEvent{id: 11, seq: 11},
					testEvent{id: 12, seq: 12},
					testEvent{id: 13, seq: 13},
				},
			},
		},
		{
			name: "non-existed",
			req: fetchRequest{
				from:  9,
				limit: 3,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}},
				{event: testEvent{id: 13, seq: 13}},
				{event: testEvent{id: 10, seq: 10}},
				{event: testEvent{id: 11, seq: 11}},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: false,
			},
		},
		{
			name: "size-limit-accept-two-events",
			req: fetchRequest{
				from:      10,
				limit:     3,
				sizeLimit: 10,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}, size: 1},
				{event: testEvent{id: 13, seq: 13}, size: 2},
				{event: testEvent{id: 10, seq: 10}, size: 3},
				{event: testEvent{id: 11, seq: 11}, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
					testEvent{id: 11, seq: 11},
				},
			},
		},
		{
			name: "size-limit-smaller-than-first-event",
			req: fetchRequest{
				from:      10,
				limit:     3,
				sizeLimit: 4,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}, size: 1},
				{event: testEvent{id: 13, seq: 13}, size: 2},
				{event: testEvent{id: 10, seq: 10}, size: 5},
				{event: testEvent{id: 11, seq: 11}, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
				},
			},
		},
		{
			name: "size-limit-just-equal-first-event",
			req: fetchRequest{
				from:      10,
				limit:     3,
				sizeLimit: 5,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}, size: 1},
				{event: testEvent{id: 13, seq: 13}, size: 2},
				{event: testEvent{id: 10, seq: 10}, size: 5},
				{event: testEvent{id: 11, seq: 11}, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
				},
			},
		},
		{
			name: "size-limit-two-events",
			req: fetchRequest{
				from:      10,
				limit:     3,
				sizeLimit: 19,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}, size: 8},
				{event: testEvent{id: 13, seq: 13}, size: 2},
				{event: testEvent{id: 10, seq: 10}, size: 5},
				{event: testEvent{id: 11, seq: 11}, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
					testEvent{id: 11, seq: 11},
				},
			},
		},
		{
			name: "size-limit-three-events",
			req: fetchRequest{
				from:      10,
				limit:     3,
				sizeLimit: 20,
			},
			stored: []storedEvent{
				{event: testEvent{id: 12, seq: 12}, size: 8},
				{event: testEvent{id: 13, seq: 13}, size: 2},
				{event: testEvent{id: 10, seq: 10}, size: 5},
				{event: testEvent{id: 11, seq: 11}, size: 7},
			},
			first: 10,
			last:  14,
			resp: fetchResponse{
				existed: true,
				events: []UnmarshalledEvent{
					testEvent{id: 10, seq: 10},
					testEvent{id: 11, seq: 11},
					testEvent{id: 12, seq: 12},
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
		waitList []fetchRequest
		last     uint64
		offset   int
		expected []fetchRequest
	}{
		{
			name: "no-remove",
			waitList: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   8,
			offset: 3,
			expected: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
		},
		{
			name: "single-remove",
			waitList: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   9,
			offset: 2,
			expected: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 3, from: 9},
				{limit: 2, from: 8},
			},
		},
		{
			name: "two-remove",
			waitList: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   10,
			offset: 1,
			expected: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 3, from: 9},
				{limit: 2, from: 8},
			},
		},
		{
			name: "all-remove",
			waitList: []fetchRequest{
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
			},
			last:   11,
			offset: 0,
			expected: []fetchRequest{
				{limit: 2, from: 8},
				{limit: 3, from: 9},
				{limit: 1, from: 10},
			},
		},
		{
			name: "normal",
			waitList: []fetchRequest{
				{limit: 0, from: 7},
				{limit: 1, from: 10},
				{limit: 2, from: 8},
				{limit: 3, from: 9},
				{limit: 4, from: 11},
				{limit: 5, from: 10},
			},
			last:   10,
			offset: 3,
			expected: []fetchRequest{
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
			offset := waitListRemoveIf(e.waitList, func(req fetchRequest) bool {
				return req.from < e.last
			})
			assert.Equal(t, e.offset, offset)
			assert.Equal(t, e.expected, e.waitList)
		})
	}
}
