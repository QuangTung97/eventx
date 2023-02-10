package eventx

import (
	"context"
)

type fetchRequest[E EventConstraint] struct {
	from        uint64
	limit       uint64
	sizeLimit   uint64
	placeholder []E
	respChan    chan<- fetchResponse[E]
}

type fetchResponse[E EventConstraint] struct {
	existed bool
	events  []E
}

type coreService[E EventConstraint] struct {
	coreChan  <-chan []E
	fetchChan chan fetchRequest[E]

	storedEvents []E
	first        uint64
	last         uint64

	waitList []fetchRequest[E]
}

func newCoreService[E EventConstraint](coreChan <-chan []E, options eventxOptions) *coreService[E] {
	return &coreService[E]{
		coreChan:  coreChan,
		fetchChan: make(chan fetchRequest[E], 256),

		storedEvents: make([]E, options.coreStoredEventsSize),
		first:        0,
		last:         0,
	}
}

func computeRequestToResponse[E EventConstraint](
	req fetchRequest[E], first uint64, last uint64, storedEvents []E,
) fetchResponse[E] {
	if req.from < first {
		return fetchResponse[E]{
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
		event := storedEvents[i%storeSize]
		size += event.GetSize()
		if i > req.from && req.sizeLimit > 0 && size > req.sizeLimit {
			break
		}

		events = append(events, event)
	}
	return fetchResponse[E]{
		existed: true,
		events:  events,
	}
}

func (s *coreService[E]) requestToResponse(req fetchRequest[E]) {
	resp := computeRequestToResponse(req, s.first, s.last, s.storedEvents)
	req.respChan <- resp
}

func waitListRemoveIf[E EventConstraint](waitList []fetchRequest[E], fn func(req fetchRequest[E]) bool) int {
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

func (s *coreService[E]) handleWaitList() {
	offset := waitListRemoveIf(s.waitList, func(req fetchRequest[E]) bool {
		return req.from < s.last
	})
	for i, waitReq := range s.waitList[offset:] {
		s.requestToResponse(waitReq)
		s.waitList[i] = fetchRequest[E]{}
	}
	s.waitList = s.waitList[:offset]
}

func (s *coreService[E]) runCore(ctx context.Context) {
	select {
	case events := <-s.coreChan:
		firstSeq := events[0].GetSequence()
		if s.first == 0 || firstSeq != s.last {
			s.first = firstSeq
		}
		s.last = events[len(events)-1].GetSequence() + 1

		size := uint64(len(s.storedEvents))
		if s.last > s.first+size {
			s.first = s.last - size
		}

		for _, e := range events {
			s.storedEvents[e.GetSequence()%size] = e
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

func (s *coreService[E]) doFetch(req fetchRequest[E]) {
	s.fetchChan <- req
}
