package eventx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSizeLimitedRepo_WithError(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
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
	var expected []testEvent
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Not_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 201)

	ctx := context.Background()

	var getFrom uint64
	var getLimit uint64

	// FIRST CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 101,
			},
		}, nil
	}

	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
		{
			id:   21,
			seq:  21,
			size: 101,
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   22,
				seq:  22,
				size: 100,
			},
			{
				id:   23,
				seq:  23,
				size: 101,
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 22)

	assert.Equal(t, uint64(22), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   22,
			seq:  22,
			size: 100,
		},
		{
			id:   23,
			seq:  23,
			size: 101,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 101,
			},
		}, nil
	}

	ctx := context.Background()
	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Single_Event_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 201,
			},
		}, nil
	}

	ctx := context.Background()
	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 201,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Near_Reach_Size_Limit(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 201)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 101,
			},
		}, nil
	}

	ctx := context.Background()
	events, err := r.getEventsFrom(ctx, 20)

	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
		{
			id:   21,
			seq:  21,
			size: 101,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit_Second_Calls(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	var getFrom uint64
	var getLimit uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 101,
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		getLimit = limit
		return []testEvent{
			{
				id:   22,
				seq:  22,
				size: 50,
			},
			{
				id:   23,
				seq:  23,
				size: 50,
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 21)

	assert.Equal(t, uint64(22), getFrom)
	assert.Equal(t, uint64(10), getLimit)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   21,
			seq:  21,
			size: 101,
		},
		{
			id:   22,
			seq:  22,
			size: 50,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit_Second_Calls_Still_Suffice_In_Memory_Size_Fitted(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 60,
			},
			{
				id:   22,
				seq:  22,
				size: 41,
			},
			{
				id:   23,
				seq:  23,
				size: 89,
			},
			{
				id:   24,
				seq:  24,
				size: 70,
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
		{
			id:   21,
			seq:  21,
			size: 60,
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	events, err = r.getEventsFrom(ctx, 22)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   22,
			seq:  22,
			size: 41,
		},
		{
			id:   23,
			seq:  23,
			size: 89,
		},
		{
			id:   24,
			seq:  24,
			size: 70,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Event_Reach_Size_Limit_Second_Calls_Still_Suffice_In_Memory_Size_Greater(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 60,
			},
			{
				id:   22,
				seq:  22,
				size: 41,
			},
			{
				id:   23,
				seq:  23,
				size: 89,
			},
			{
				id:   24,
				seq:  24,
				size: 71,
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
		{
			id:   21,
			seq:  21,
			size: 60,
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	events, err = r.getEventsFrom(ctx, 22)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   22,
			seq:  22,
			size: 41,
		},
		{
			id:   23,
			seq:  23,
			size: 89,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Third_Calls(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	var getFrom uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 60,
			},
			{
				id:   22,
				seq:  22,
				size: 41,
			},
			{
				id:   23,
				seq:  23,
				size: 89,
			},
			{
				id:   24,
				seq:  24,
				size: 71,
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
		{
			id:   21,
			seq:  21,
			size: 60,
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	events, err = r.getEventsFrom(ctx, 22)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   22,
			seq:  22,
			size: 41,
		},
		{
			id:   23,
			seq:  23,
			size: 89,
		},
	}
	assert.Equal(t, expected, events)

	// THIRD CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		return []testEvent{
			{
				id:   25,
				seq:  25,
				size: 80,
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 24)
	assert.Equal(t, uint64(25), getFrom)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   24,
			seq:  24,
			size: 71,
		},
		{
			id:   25,
			seq:  25,
			size: 80,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_With_Spacing__Match_Size(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}

	r := newSizeLimitedRepo[testEvent](repo, 10, 200)

	var getFrom uint64
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		getFrom = from
		return []testEvent{
			{
				id:   20,
				seq:  20,
				size: 100,
			},
			{
				id:   21,
				seq:  21,
				size: 60,
			},
			{
				id:   22,
				seq:  22,
				size: 41,
			},
			{
				id:   23,
				seq:  23,
				size: 89,
			},
			{
				id:   24,
				seq:  24,
				size: 111,
			},
		}, nil
	}

	ctx := context.Background()

	// FIRST CALL
	events, err := r.getEventsFrom(ctx, 20)
	assert.Equal(t, uint64(20), getFrom)
	assert.Equal(t, 1, len(repo.GetEventsFromCalls()))

	assert.Equal(t, nil, err)
	expected := []testEvent{
		{
			id:   20,
			seq:  20,
			size: 100,
		},
		{
			id:   21,
			seq:  21,
			size: 60,
		},
	}
	assert.Equal(t, expected, events)

	// SECOND CALL
	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]testEvent, error) {
		return []testEvent{
			{
				id:   23,
				seq:  23,
				size: 89,
			},
			{
				id:   24,
				seq:  24,
				size: 111,
			},
		}, nil
	}

	events, err = r.getEventsFrom(ctx, 23)
	assert.Equal(t, 2, len(repo.GetEventsFromCalls()))
	assert.Equal(t, nil, err)
	expected = []testEvent{
		{
			id:   23,
			seq:  23,
			size: 89,
		},
		{
			id:   24,
			seq:  24,
			size: 111,
		},
	}
	assert.Equal(t, expected, events)
}

func TestSizeLimitedRepo_GetFromMem_Empty(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)
	events, from, ok := r.getFromMem(20)
	assert.Equal(t, []testEvent(nil), events)
	assert.Equal(t, uint64(20), from)
	assert.Equal(t, false, ok)
}

func TestSizeLimitedRepo_GetFromMem_SingleElement(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 60},
	})

	events, from, ok := r.getFromMem(20)

	assert.Equal(t, []testEvent(nil), events)
	assert.Equal(t, uint64(21), from)
	assert.Equal(t, false, ok)
}

func TestSizeLimitedRepo_Fit_Size_Limit(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 40},
	})

	events, from, ok := r.getFromMem(20)

	assert.Equal(t, []testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 40},
	}, events)
	assert.Equal(t, uint64(0), from)
	assert.Equal(t, true, ok)
	assert.Equal(t, []testEvent{}, r.lastEvents)
}

func TestSizeLimitedRepo_Exceed_Size_Limit(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
		{id: 22, seq: 22, size: 21},
		{id: 23, seq: 23, size: 40},
	})

	events, from, ok := r.getFromMem(20)

	assert.Equal(t, []testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
	}, events)
	assert.Equal(t, uint64(0), from)
	assert.Equal(t, true, ok)
	assert.Equal(t, []testEvent{
		{id: 22, seq: 22, size: 21},
		{id: 23, seq: 23, size: 40},
	}, r.lastEvents)
}

func TestSizeLimitedRepo_GetFromMem_From_Not_Match_First_Element(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
		{id: 22, seq: 22, size: 21},
		{id: 23, seq: 23, size: 59},
	})

	events, from, ok := r.getFromMem(21)

	assert.Equal(t, []testEvent(nil), events)
	assert.Equal(t, uint64(21), from)
	assert.Equal(t, false, ok)
	assert.Equal(t, []testEvent(nil), r.lastEvents)
}

func TestSizeLimitedRepo_GetFromMem_Reach_Limit(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 4},
		{id: 21, seq: 21, size: 4},
		{id: 22, seq: 22, size: 4},
		{id: 23, seq: 23, size: 4},
		{id: 24, seq: 24, size: 4},
		{id: 25, seq: 25, size: 4},
	})

	events, from, ok := r.getFromMem(20)
	assert.Equal(t, uint64(0), from)
	assert.Equal(t, true, ok)

	assert.Equal(t, []testEvent{
		{id: 20, seq: 20, size: 4},
		{id: 21, seq: 21, size: 4},
		{id: 22, seq: 22, size: 4},
		{id: 23, seq: 23, size: 4},
		{id: 24, seq: 24, size: 4},
	}, events)
	assert.Equal(t, []testEvent{
		{id: 25, seq: 25, size: 4},
	}, r.lastEvents)
}

func TestSizeLimitedRepo_ForceGetFromMem_NotReachSizeLimit(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
	})

	events := r.forceGetFromMem()

	assert.Equal(t, []testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
	}, events)
	assert.Equal(t, []testEvent(nil), r.lastEvents)
}

func TestSizeLimitedRepo_ForceGetFromMem_ExceedSizeLimit(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
		{id: 21, seq: 21, size: 21},
	})

	events := r.forceGetFromMem()

	assert.Equal(t, []testEvent{
		{id: 20, seq: 20, size: 60},
		{id: 21, seq: 21, size: 20},
	}, events)
}

func TestSizeLimitedRepo_ForceGetFromMem_Exceed_Size_Limit_At_First_Element(t *testing.T) {
	r := newSizeLimitedRepo[testEvent](nil, 5, 100)

	r.setDBResult([]testEvent{
		{id: 20, seq: 20, size: 101},
	})

	events := r.forceGetFromMem()

	assert.Equal(t, []testEvent{
		{id: 20, seq: 20, size: 101},
	}, events)
}
