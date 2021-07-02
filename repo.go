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

func (r *sizeLimitedRepo) getLastEventsFrom(index int) []Event {
	result := r.lastEvents[:index]
	r.lastEvents = r.lastEvents[index:]
	return result
}

func (r *sizeLimitedRepo) returnsFromLastEvents() ([]Event, bool) {
	size := uint64(0)
	for i, e := range r.lastEvents {
		size += uint64(len(e.Data))
		if i == 0 {
			continue
		}

		if size == r.sizeLimit {
			return r.getLastEventsFrom(i + 1), true
		}
		if size > r.sizeLimit {
			return r.getLastEventsFrom(i), true
		}
	}
	return nil, false
}

func (r *sizeLimitedRepo) getEventsFrom(ctx context.Context, from uint64) ([]Event, error) {
	events, ok := r.returnsFromLastEvents()
	if ok {
		return events, nil
	}

	if len(r.lastEvents) > 0 {
		from = r.lastEvents[len(r.lastEvents)-1].Seq + 1
	}

	dbEvents, err := r.repo.GetEventsFrom(ctx, from, r.limit)
	if err != nil {
		return nil, err
	}

	r.lastEvents = append(r.lastEvents, dbEvents...)

	events, ok = r.returnsFromLastEvents()
	if ok {
		return events, nil
	}

	return r.getLastEventsFrom(len(r.lastEvents)), nil
}
