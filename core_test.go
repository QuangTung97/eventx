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
			unmarshalEvent(Event{ID: 10, Seq: 20}),
			unmarshalEvent(Event{ID: 8, Seq: 21}),
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
			unmarshalEvent(Event{ID: 8, Seq: 30}),
			unmarshalEvent(Event{ID: 11, Seq: 31}),
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
			unmarshalEvent(Event{ID: 8, Seq: 30}),
			unmarshalEvent(Event{ID: 11, Seq: 31}),
			unmarshalEvent(Event{ID: 15, Seq: 32}),
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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
	s := newCoreService(coreChan, computeOptions(WithCoreStoredEventsSize(4)))

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
