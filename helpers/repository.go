package helpers

import (
	"context"
	"github.com/QuangTung97/eventx"
	"github.com/stretchr/testify/assert"
	"testing"
)

// CheckRepoImpl ..
//
//revive:disable-next-line:argument-limit
func CheckRepoImpl[E eventx.EventConstraint](
	t *testing.T,
	repo eventx.Repository[E],

	setID func(e *E, id int64),
	setSequence func(e *E, seq uint64),

	randomEvent func() E,
	insertEvents func(events []E),
	truncateFunc func(),
) {
	t.Run("insert-then-get-unprocessed--then-update--then-get-get-events-from", func(t *testing.T) {
		truncateFunc()

		events, err := repo.GetLastEvents(context.Background(), 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(events))

		ev1 := randomEvent()
		ev2 := randomEvent()
		ev3 := randomEvent()
		ev4 := randomEvent()
		ev5 := randomEvent()

		insertEvents([]E{ev1, ev2, ev3, ev4, ev5})

		setID(&ev1, 1)
		setID(&ev2, 2)
		setID(&ev3, 3)
		setID(&ev4, 4)
		setID(&ev5, 5)

		events, err = repo.GetUnprocessedEvents(context.Background(), 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev1, ev2, ev3, ev4, ev5}, events)

		// With Smaller Limit
		events, err = repo.GetUnprocessedEvents(context.Background(), 3)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev1, ev2, ev3}, events)

		setSequence(&ev1, 15)
		setSequence(&ev2, 14)
		setSequence(&ev3, 12)
		setSequence(&ev4, 13)
		setSequence(&ev5, 11)

		// Update All Sequences
		err = repo.UpdateSequences(context.Background(), []E{ev1, ev2, ev3, ev4, ev5})
		assert.Equal(t, nil, err)

		// Get Unprocessed Events
		events, err = repo.GetUnprocessedEvents(context.Background(), 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(events))

		// Get Events From
		events, err = repo.GetEventsFrom(context.Background(), 0, 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev5, ev3, ev4, ev2, ev1}, events)

		events, err = repo.GetEventsFrom(context.Background(), 13, 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev4, ev2, ev1}, events)

		events, err = repo.GetEventsFrom(context.Background(), 12, 3)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev3, ev4, ev2}, events)

		// Get Last Events
		events, err = repo.GetLastEvents(context.Background(), 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev5, ev3, ev4, ev2, ev1}, events)

		events, err = repo.GetLastEvents(context.Background(), 3)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev4, ev2, ev1}, events)
	})

	t.Run("update-partial", func(t *testing.T) {
		truncateFunc()

		ev1 := randomEvent()
		ev2 := randomEvent()
		ev3 := randomEvent()
		ev4 := randomEvent()
		ev5 := randomEvent()

		insertEvents([]E{ev1, ev2, ev3, ev4, ev5})

		setID(&ev1, 1)
		setID(&ev2, 2)
		setID(&ev3, 3)
		setID(&ev4, 4)
		setID(&ev5, 5)

		events, err := repo.GetUnprocessedEvents(context.Background(), 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev1, ev2, ev3, ev4, ev5}, events)

		setSequence(&ev1, 13)
		setSequence(&ev2, 11)
		setSequence(&ev3, 12)

		// Update All Sequences
		err = repo.UpdateSequences(context.Background(), []E{ev1, ev2, ev3})
		assert.Equal(t, nil, err)

		events, err = repo.GetUnprocessedEvents(context.Background(), 10)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev4, ev5}, events)

		events, err = repo.GetUnprocessedEvents(context.Background(), 1)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev4}, events)

		// Get Last Events
		events, err = repo.GetLastEvents(context.Background(), 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, []E{ev3, ev1}, events)
	})
}
