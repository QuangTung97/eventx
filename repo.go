package eventx

import "context"

type sizeLimitedRepo struct {
	repo       Repository
	limit      uint64
	sizeLimit  uint64
	lastEvents []Event
}

func newSizeLimitedRepo(repo Repository, limit uint64, sizeLimit uint64) *sizeLimitedRepo {
	return &sizeLimitedRepo{
		repo:      repo,
		limit:     limit,
		sizeLimit: sizeLimit,
	}
}

func (r *sizeLimitedRepo) splitLastEvents(index int) []Event {
	result := r.lastEvents[:index]
	r.lastEvents = r.lastEvents[index:]
	return result
}

func (r *sizeLimitedRepo) getEventsFrom(ctx context.Context, from uint64) ([]Event, error) {
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

func (r *sizeLimitedRepo) tryToGetFromMem() ([]Event, bool) {
	size := uint64(0)
	for i, event := range r.lastEvents {
		if uint64(i) >= r.limit {
			return r.splitLastEvents(i), true
		}

		size += uint64(len(event.Data))

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

func (r *sizeLimitedRepo) getFromMem(from uint64) ([]Event, uint64, bool) {
	if len(r.lastEvents) == 0 {
		return nil, from, false
	}

	firstSeq := r.lastEvents[0].Seq
	if firstSeq != from {
		r.lastEvents = nil
		return nil, from, false
	}

	events, ok := r.tryToGetFromMem()
	if ok {
		return events, 0, true
	}

	lastSeq := r.lastEvents[len(r.lastEvents)-1].Seq
	return nil, lastSeq + 1, false
}

func (r *sizeLimitedRepo) setDBResult(events []Event) {
	r.lastEvents = append(r.lastEvents, events...)
}

func (r *sizeLimitedRepo) forceGetFromMem() []Event {
	events, ok := r.tryToGetFromMem()
	if ok {
		return events
	}

	events = r.lastEvents
	r.lastEvents = nil
	return events
}
