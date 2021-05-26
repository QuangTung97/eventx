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

func TestCoreServiceFetch_SimpleCase(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreService(coreChan, computeOptions())

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
			{seq: 20},
			{seq: 21},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceRun_ContextCancel(t *testing.T) {
	t.Parallel()

	s := newCoreService(nil, computeOptions())

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	s.run(ctx)
}

func TestCoreServiceFetch_Behind(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreService(coreChan, computeOptions())

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
	s := newCoreService(coreChan, computeOptions())

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
			{seq: 30},
			{seq: 31},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_Limit_Exceed(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreService(coreChan, computeOptions())

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
			{seq: 30},
			{seq: 31},
			{seq: 32},
		},
	}, resp)
	assert.Equal(t, 100, cap(resp.events))
}

func TestCoreServiceFetch_Wrap_Around_And_Override_Not_Exist(t *testing.T) {
	t.Parallel()

	coreChan := make(chan coreEvents, 1024)
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
			{seq: 31},
			{seq: 32},
			{seq: 33},
			{seq: 34},
		},
	}, resp)
}