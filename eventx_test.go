package eventx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestSubscriber(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
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
	assert.Equal(t, []testEvent{
		{id: 33, seq: 20},
		{id: 32, seq: 21},
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_WithSizeLimit(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18, size: 10},
			{id: 28, seq: 19, size: 11},
			{id: 33, seq: 20, size: 12},
			{id: 32, seq: 21, size: 13},
		}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(18, 5, WithSubscriberSizeLimit(32))
	events, err := sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []testEvent{
		{id: 30, seq: 18, size: 10},
		{id: 28, seq: 19, size: 11},
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_Context_Cancelled_Continue(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18, size: 10},
			{id: 28, seq: 19, size: 11},
			{id: 33, seq: 20, size: 12},
			{id: 32, seq: 21, size: 13},
		}, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(18, 2)

	fetchCtx, fetchCancel := context.WithCancel(context.Background())
	fetchCancel()

	events, err := sub.Fetch(fetchCtx)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, []testEvent(nil), events)

	events, err = sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []testEvent{
		{id: 30, seq: 18, size: 10},
		{id: 28, seq: 19, size: 11},
	}, events)

	events, err = sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []testEvent{
		{id: 33, seq: 20, size: 12},
		{id: 32, seq: 21, size: 13},
	}, events)

	cancel()
	wg.Wait()

	assert.Equal(t, 0, len(sub.respChan))
}

func TestSubscriber_Not_Existed(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 10, seq: 17},
			{id: 12, seq: 18},
			{id: 11, seq: 19},
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
	assert.Equal(t, []testEvent{
		{id: 10, seq: 17},
		{id: 12, seq: 18},
		{id: 11, seq: 19},
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_Fetch_In_Mem_After_Access_DB(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 10, seq: 17},
			{id: 30, seq: 18},
			{id: 28, seq: 19},
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
	assert.Equal(t, []testEvent{
		{id: 10, seq: 17},
		{id: 30, seq: 18},
		{id: 28, seq: 19},
	}, events)

	events, err = sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []testEvent{
		{id: 33, seq: 20},
		{id: 32, seq: 21},
	}, events)

	cancel()
	wg.Wait()
}

func TestSubscriber_GetEventsFrom_Returns_Error(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return nil, errors.New("get-events-error")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(17, 5)
	events, err := sub.Fetch(ctx)
	assert.Equal(t, errors.New("get-events-error"), err)
	assert.Equal(t, []testEvent(nil), events)
	assert.Equal(t, uint64(17), sub.from)

	cancel()
	wg.Wait()
}

func TestSubscriber_GetEventsFrom_Returns_Empty(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		r.Run(ctx)
	}()

	sub := r.NewSubscriber(17, 5)
	events, err := sub.Fetch(ctx)
	assert.Equal(t, ErrEventNotFound, err)
	assert.Equal(t, []testEvent(nil), events)
	assert.Equal(t, uint64(17), sub.from)

	cancel()
	wg.Wait()
}

func TestSubscriber_GetEventsFrom__Returns_Missing_First_Sequence(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
		}, nil
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
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
	assert.Equal(t, ErrEventNotFound, err)
	assert.Equal(t, []testEvent(nil), events)
	assert.Equal(t, uint64(17), sub.from)

	cancel()
	wg.Wait()
}

func TestSubscriber_Multiple_Fetch(t *testing.T) {
	repo := &RepositoryMock[testEvent]{}
	r := NewRunner[testEvent](repo, setTestEventSeq)

	ctx, cancel := context.WithCancel(context.Background())

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{id: 30, seq: 18},
			{id: 28, seq: 19},
			{id: 33, seq: 20},
			{id: 32, seq: 21},
			{id: 40, seq: 22},
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
	assert.Equal(t, []testEvent{
		{id: 30, seq: 18},
		{id: 28, seq: 19},
		{id: 33, seq: 20},
	}, events)

	events, err = sub.Fetch(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []testEvent{
		{id: 32, seq: 21},
		{id: 40, seq: 22},
	}, events)

	cancel()
	wg.Wait()
}

func TestMergeContext(t *testing.T) {
	t.Run("both-background", func(t *testing.T) {
		a := context.Background()
		b := context.Background()
		result := MergeContext(a, b)
		assert.Equal(t, nil, result.Err())
	})

	t.Run("a-cancelled", func(t *testing.T) {
		a, cancel := context.WithCancel(context.Background())
		cancel()
		b := context.Background()
		result := MergeContext(a, b)
		assert.Equal(t, context.Canceled, result.Err())
	})

	t.Run("b-cancelled", func(t *testing.T) {
		a := context.Background()
		b, cancel := context.WithCancel(context.Background())
		cancel()

		result := MergeContext(a, b)
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, context.Canceled, result.Err())
	})
}
