package eventx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSizeLimitedRepo_WithError(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return nil, errors.New("get-events-error")
	}

	ctx := context.Background()
	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, errors.New("get-events-error"), err)
	var expected []Event
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Not_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 201)

	ctx := context.Background()

	var getFrom uint64
	var getLimit uint64

	// FIRST CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(101),
			},
		}, nil
	}

	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
		{
			ID:   21,
			Seq:  21,
			Data: stringSize(101),
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return []Event{
			{
				ID:   22,
				Seq:  22,
				Data: stringSize(100),
			},
			{
				ID:   23,
				Seq:  23,
				Data: stringSize(101),
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected = []Event{
		{
			ID:   22,
			Seq:  22,
			Data: stringSize(100),
		},
		{
			ID:   23,
			Seq:  23,
			Data: stringSize(101),
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(101),
			},
		}, nil
	}

	ctx := context.Background()
	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Near_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 201)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(101),
			},
		}, nil
	}

	ctx := context.Background()
	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
		{
			ID:   21,
			Seq:  21,
			Data: stringSize(101),
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit_Second_Calls(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(101),
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		getLimit = limit
		return []Event{
			{
				ID:   22,
				Seq:  22,
				Data: stringSize(50),
			},
			{
				ID:   23,
				Seq:  23,
				Data: stringSize(50),
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 21)

	assert.Equal(t, uint64(22), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected = []Event{
		{
			ID:   21,
			Seq:  21,
			Data: stringSize(101),
		},
		{
			ID:   22,
			Seq:  22,
			Data: stringSize(50),
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit_Second_Calls_Still_Suffice_In_Memory_Size_Fitted(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 200)

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(60),
			},
			{
				ID:   22,
				Seq:  22,
				Data: stringSize(41),
			},
			{
				ID:   23,
				Seq:  23,
				Data: stringSize(89),
			},
			{
				ID:   24,
				Seq:  24,
				Data: stringSize(70),
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
		{
			ID:   21,
			Seq:  21,
			Data: stringSize(60),
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	events, err = r.getEventsFrom(ctx, 21)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []Event{
		{
			ID:   22,
			Seq:  22,
			Data: stringSize(41),
		},
		{
			ID:   23,
			Seq:  23,
			Data: stringSize(89),
		},
		{
			ID:   24,
			Seq:  24,
			Data: stringSize(70),
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit_Second_Calls_Still_Suffice_In_Memory_Size_Greater(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 200)

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(60),
			},
			{
				ID:   22,
				Seq:  22,
				Data: stringSize(41),
			},
			{
				ID:   23,
				Seq:  23,
				Data: stringSize(89),
			},
			{
				ID:   24,
				Seq:  24,
				Data: stringSize(71),
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
		{
			ID:   21,
			Seq:  21,
			Data: stringSize(60),
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	events, err = r.getEventsFrom(ctx, 21)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []Event{
		{
			ID:   22,
			Seq:  22,
			Data: stringSize(41),
		},
		{
			ID:   23,
			Seq:  23,
			Data: stringSize(89),
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Third_Calls(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}

	r := newSizeLimitedRepo(repo, 10, 200)

	var getFrom uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		return []Event{
			{
				ID:   20,
				Seq:  20,
				Data: stringSize(100),
			},
			{
				ID:   21,
				Seq:  21,
				Data: stringSize(60),
			},
			{
				ID:   22,
				Seq:  22,
				Data: stringSize(41),
			},
			{
				ID:   23,
				Seq:  23,
				Data: stringSize(89),
			},
			{
				ID:   24,
				Seq:  24,
				Data: stringSize(71),
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []Event{
		{
			ID:   20,
			Seq:  20,
			Data: stringSize(100),
		},
		{
			ID:   21,
			Seq:  21,
			Data: stringSize(60),
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	events, err = r.getEventsFrom(ctx, 22)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []Event{
		{
			ID:   22,
			Seq:  22,
			Data: stringSize(41),
		},
		{
			ID:   23,
			Seq:  23,
			Data: stringSize(89),
		},
	}
	assert.Equal(t, expected, events)

	// THIRD CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		getFrom = from
		return []Event{
			{
				ID:   25,
				Seq:  25,
				Data: stringSize(80),
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 24)
	assert.Equal(t, uint64(25), getFrom)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected = []Event{
		{
			ID:   24,
			Seq:  24,
			Data: stringSize(71),
		},
		{
			ID:   25,
			Seq:  25,
			Data: stringSize(80),
		},
	}
	assert.Equal(t, expected, events)
}
