package eventx

import (
	"context"
	"google.golang.org/protobuf/proto"
)

type fetchRequest struct {
	from        uint64
	limit       uint64
	sizeLimit   uint64
	placeholder []proto.Message
	respChan    chan<- fetchResponse
}

type fetchResponse struct {
	existed bool
	events  []proto.Message
}

type storedEvent struct {
	event proto.Message
	size  uint64
}

type coreService struct {
	coreChan  <-chan coreEvents
	fetchChan chan fetchRequest
	unmarshal UnmarshalEvent

	storedEvents []storedEvent
	first        uint64
	last         uint64

	waitList []fetchRequest
}

func newCoreService(coreChan <-chan coreEvents, unmarshal UnmarshalEvent, options eventxOptions) *coreService {
	return &coreService{
		coreChan:  coreChan,
		fetchChan: make(chan fetchRequest, 256),
		unmarshal: unmarshal,

		storedEvents: make([]storedEvent, options.coreStoredEventsSize),
		first:        0,
		last:         0,
	}
}

func computeRequestToResponse(
	req fetchRequest, first uint64, last uint64, storedEvents []storedEvent,
) fetchResponse {
	if req.from < first {
		return fetchResponse{
			existed: false,
		}
	}

	events := req.placeholder

	end := req.from + req.limit
	if end > last {
		end = last
	}

	size := uint64(0)
	storeSize := uint64(len(storedEvents))
	for i := req.from; i < end; i++ {
		stored := storedEvents[i%storeSize]
		size += stored.size
		if i > req.from && req.sizeLimit > 0 && size > req.sizeLimit {
			break
		}

		events = append(events, stored.event)
	}
	return fetchResponse{
		existed: true,
		events:  events,
	}
}

func (s *coreService) requestToResponse(req fetchRequest) {
	resp := computeRequestToResponse(req, s.first, s.last, s.storedEvents)
	req.respChan <- resp
}

func waitListRemoveIf(waitList []fetchRequest, fn func(req fetchRequest) bool) int {
	clearIndex := len(waitList)
	for i := 0; i < clearIndex; {
		req := waitList[i]
		if !fn(req) {
			i++
			continue
		}
		clearIndex--
		waitList[i], waitList[clearIndex] = waitList[clearIndex], waitList[i]
	}
	return clearIndex
}

func (s *coreService) handleWaitList() {
	offset := waitListRemoveIf(s.waitList, func(req fetchRequest) bool {
		return req.from < s.last
	})
	for i, waitReq := range s.waitList[offset:] {
		s.requestToResponse(waitReq)
		s.waitList[i] = fetchRequest{}
	}
	s.waitList = s.waitList[:offset]
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
			s.storedEvents[e.Seq%size] = storedEvent{
				event: s.unmarshal(e),
				size:  uint64(len(e.Data)),
			}
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
