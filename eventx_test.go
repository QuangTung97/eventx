package eventx

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestSubscriber(t *testing.T) {
	repo := &RepositoryMock{}
	r := NewRunner(repo)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 30, Seq: 18},
			{ID: 28, Seq: 19},
			{ID: 33, Seq: 20},
			{ID: 32, Seq: 21},
		}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(20, 5)
	events, err := sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []UnmarshalledEvent{
		unmarshalEvent(Event{ID: 33, Seq: 20}),
		unmarshalEvent(Event{ID: 32, Seq: 21}),
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_Not_Existed(t *testing.T) {
	repo := &RepositoryMock{}
	r := NewRunner(repo)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 30, Seq: 18},
			{ID: 28, Seq: 19},
			{ID: 33, Seq: 20},
			{ID: 32, Seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10, Seq: 17},
			{ID: 12, Seq: 18},
			{ID: 11, Seq: 19},
		}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(17, 5)
	events, err := sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []UnmarshalledEvent{
		unmarshalEvent(Event{ID: 10, Seq: 17}),
		unmarshalEvent(Event{ID: 12, Seq: 18}),
		unmarshalEvent(Event{ID: 11, Seq: 19}),
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_Fetch_In_Mem_After_Access_DB(t *testing.T) {
	repo := &RepositoryMock{}
	r := NewRunner(repo)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 30, Seq: 18},
			{ID: 28, Seq: 19},
			{ID: 33, Seq: 20},
			{ID: 32, Seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10, Seq: 17},
			{ID: 30, Seq: 18},
			{ID: 28, Seq: 19},
		}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(17, 5)
	events, err := sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []UnmarshalledEvent{
		unmarshalEvent(Event{ID: 10, Seq: 17}),
		unmarshalEvent(Event{ID: 30, Seq: 18}),
		unmarshalEvent(Event{ID: 28, Seq: 19}),
	}, events)

	events, err = sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []UnmarshalledEvent{
		unmarshalEvent(Event{ID: 33, Seq: 20}),
		unmarshalEvent(Event{ID: 32, Seq: 21}),
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_Multiple_Fetch(t *testing.T) {
	repo := &RepositoryMock{}
	r := NewRunner(repo)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 30, Seq: 18},
			{ID: 28, Seq: 19},
			{ID: 33, Seq: 20},
			{ID: 32, Seq: 21},
			{ID: 40, Seq: 22},
		}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(18, 3)
	events, err := sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []UnmarshalledEvent{
		unmarshalEvent(Event{ID: 30, Seq: 18}),
		unmarshalEvent(Event{ID: 28, Seq: 19}),
		unmarshalEvent(Event{ID: 33, Seq: 20}),
	}, events)

	events, err = sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []UnmarshalledEvent{
		unmarshalEvent(Event{ID: 32, Seq: 21}),
		unmarshalEvent(Event{ID: 40, Seq: 22}),
	}, events)

	cancel()
	wg.Wait()
}
