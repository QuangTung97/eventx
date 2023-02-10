package eventx

import "context"

type sizeLimitedRepo[E EventConstraint] struct {
	repo       Repository[E]
	limit      uint64
	sizeLimit  uint64
	lastEvents []E
}

func newSizeLimitedRepo[E EventConstraint](repo Repository[E], limit uint64, sizeLimit uint64) *sizeLimitedRepo[E] {
	return &sizeLimitedRepo[E]{
		repo:      repo,
		limit:     limit,
		sizeLimit: sizeLimit,
	}
}

func (r *sizeLimitedRepo[E]) splitLastEvents(index int) []E {
	result := r.lastEvents[:index]
	r.lastEvents = r.lastEvents[index:]
	return result
}

func (r *sizeLimitedRepo[E]) getEventsFrom(ctx context.Context, from uint64) ([]E, error) {
	events, dbFrom, ok := r.getFromMem(from)
	if ok {
		return events, nil
	}

	dbEvents, err := r.repo.GetEventsFrom(ctx, dbFrom, r.limit)
	if err != nil {
		return nil, err
	}

	r.setDBResult(dbEvents)

	return r.forceGetFromMem(), nil
}

func (r *sizeLimitedRepo[E]) tryToGetFromMem() ([]E, bool) {
	size := uint64(0)
	for i, event := range r.lastEvents {
		if uint64(i) >= r.limit {
			return r.splitLastEvents(i), true
		}

		size += event.GetSize()

		if size == r.sizeLimit {
			return r.splitLastEvents(i + 1), true
		}
		if size > r.sizeLimit {
			if i > 0 {
				return r.splitLastEvents(i), true
			}
			return r.splitLastEvents(i + 1), true
		}
	}

	return nil, false
}

func (r *sizeLimitedRepo[E]) getFromMem(from uint64) ([]E, uint64, bool) {
	if len(r.lastEvents) == 0 {
		return nil, from, false
	}

	firstSeq := r.lastEvents[0].GetSequence()
	if firstSeq != from {
		r.lastEvents = nil
		return nil, from, false
	}

	events, ok := r.tryToGetFromMem()
	if ok {
		return events, 0, true
	}

	lastSeq := r.lastEvents[len(r.lastEvents)-1].GetSequence()
	return nil, lastSeq + 1, false
}

func (r *sizeLimitedRepo[E]) setDBResult(events []E) {
	r.lastEvents = append(r.lastEvents, events...)
}

func (r *sizeLimitedRepo[E]) forceGetFromMem() []E {
	events, ok := r.tryToGetFromMem()
	if ok {
		return events
	}

	events = r.lastEvents
	r.lastEvents = nil
	return events
}
