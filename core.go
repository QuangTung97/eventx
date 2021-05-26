package eventx

import (
	"context"
)

type fetchRequest struct {
	limit       uint64
	from        uint64
	placeholder []UnmarshalledEvent
	respChan    chan<- fetchResponse
}

type fetchResponse struct {
	existed bool
	events  []UnmarshalledEvent
}

type coreService struct {
	coreChan  <-chan coreEvents
	fetchChan chan fetchRequest

	storedEvents []UnmarshalledEvent
	first        uint64
	last         uint64
}

func newCoreService(coreChan <-chan coreEvents, options eventxOptions) *coreService {
	return &coreService{
		coreChan:     coreChan,
		fetchChan:    make(chan fetchRequest, 256),
		storedEvents: make([]UnmarshalledEvent, options.coreStoredEventsSize),
		first:        0,
		last:         0,
	}
}

func (s *coreService) run(ctx context.Context) {
	select {
	case events := <-s.coreChan:
		if s.first == 0 {
			s.first = events[0].Seq
		}
		s.last = events[len(events)-1].Seq + 1

		size := uint64(len(s.storedEvents))
		if s.last > s.first+size {
			s.first = s.last - size
		}

		for _, e := range events {
			s.storedEvents[e.Seq%size] = unmarshalEvent(e)
		}

	case req := <-s.fetchChan:
		if req.from < s.first {
			req.respChan <- fetchResponse{
				existed: false,
			}
			return
		}

		events := req.placeholder

		last := req.from + req.limit
		if last > s.last {
			last = s.last
		}

		size := uint64(len(s.storedEvents))
		for i := req.from; i < last; i++ {
			events = append(events, s.storedEvents[i%size])
		}
		req.respChan <- fetchResponse{
			existed: true,
			events:  events,
		}

	case <-ctx.Done():
	}
}

func (s *coreService) fetch(req fetchRequest) {
	s.fetchChan <- req
}
