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

	waitList []fetchRequest
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

func (s *coreService) requestToResponse(req fetchRequest) {
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
}

func (s *coreService) handleWaitList() {
	clearPos := len(s.waitList)
	for i, waitReq := range s.waitList {
		if waitReq.from < s.last {
			s.requestToResponse(waitReq)
			clearPos--
			s.waitList[i] = s.waitList[clearPos]
		}
	}
	for i := clearPos; i < len(s.waitList); i++ {
		s.waitList[i] = fetchRequest{}
	}
	s.waitList = s.waitList[:clearPos]
}

func (s *coreService) run(ctx context.Context) {
	select {
	case events := <-s.coreChan:
		firstSeq := events[0].Seq
		if s.first == 0 || firstSeq != s.last {
			s.first = firstSeq
		}
		s.last = events[len(events)-1].Seq + 1

		size := uint64(len(s.storedEvents))
		if s.last > s.first+size {
			s.first = s.last - size
		}

		for _, e := range events {
			s.storedEvents[e.Seq%size] = unmarshalEvent(e)
		}
		s.handleWaitList()

	case req := <-s.fetchChan:
		if req.from >= s.last {
			s.waitList = append(s.waitList, req)
			return
		}
		s.requestToResponse(req)

	case <-ctx.Done():
	}
}

func (s *coreService) fetch(req fetchRequest) {
	s.fetchChan <- req
}
