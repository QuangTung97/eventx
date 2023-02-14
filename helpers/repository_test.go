package helpers

import (
	"context"
	"math"
	"sync"
	"testing"
)

type testEvent struct {
	id   int64
	seq  uint64
	data int
}

func (e testEvent) GetSequence() uint64 {
	return e.seq
}

func (testEvent) GetSize() uint64 {
	return 0
}

type repoTest struct {
	mut    sync.Mutex
	nextID int64

	events   map[int64]testEvent
	eventIDs []int64

	seqMap map[uint64]int64
}

func newRepoTest() *repoTest {
	return &repoTest{
		nextID: 0,
		events: map[int64]testEvent{},
		seqMap: map[uint64]int64{},
	}
}

func (r *repoTest) GetLastEvents(_ context.Context, limit uint64) ([]testEvent, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	var maxSeq uint64
	var minSeq uint64 = math.MaxUint64
	for seq := range r.seqMap {
		if seq > maxSeq {
			maxSeq = seq
		}
		if seq < minSeq {
			minSeq = seq
		}
	}

	var result []testEvent

	start := int64(minSeq)
	bound := int64(maxSeq) - int64(limit) + 1
	if start < bound {
		start = bound
	}

	for seq := start; seq <= int64(maxSeq); seq++ {
		if len(result) >= int(limit) {
			break
		}
		id := r.seqMap[uint64(seq)]
		result = append(result, r.events[id])
	}

	return result, nil
}

func (r *repoTest) GetUnprocessedEvents(_ context.Context, limit uint64) ([]testEvent, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	var result []testEvent
	for _, id := range r.eventIDs {
		if len(result) >= int(limit) {
			break
		}

		ev := r.events[id]
		if ev.seq == 0 {
			result = append(result, ev)
		}
	}

	return result, nil
}

func (r *repoTest) GetEventsFrom(_ context.Context, from uint64, limit uint64) ([]testEvent, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	var result []testEvent
	for seq := from; seq < from+limit+500; seq++ {
		if len(result) >= int(limit) {
			break
		}

		id, ok := r.seqMap[seq]
		if !ok {
			continue
		}
		result = append(result, r.events[id])
	}

	return result, nil
}

func (r *repoTest) UpdateSequences(_ context.Context, events []testEvent) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	for _, e := range events {
		r.seqMap[e.seq] = e.id

		prev := r.events[e.id]
		prev.seq = e.seq
		r.events[e.id] = prev
	}

	return nil
}

func (r *repoTest) insertEvents(events []testEvent) {
	r.mut.Lock()
	for i := range events {
		r.nextID++
		id := r.nextID
		events[i].id = id
	}
	r.mut.Unlock()

	r.mut.Lock()
	defer r.mut.Unlock()

	for _, e := range events {
		r.events[e.id] = e
		r.eventIDs = append(r.eventIDs, e.id)
	}
}

func TestCheckRepoImpl(t *testing.T) {
	repo := newRepoTest()
	nextData := 188

	CheckRepoImpl[testEvent](t,
		repo,
		func(e *testEvent, id int64) {
			e.id = id
		},
		func(e *testEvent, seq uint64) {
			e.seq = seq
		},
		func() testEvent {
			nextData++
			return testEvent{
				data: nextData,
			}
		},
		repo.insertEvents,
		func() {
			*repo = *newRepoTest()
		},
	)
}
