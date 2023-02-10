package eventx

import "context"

type dbProcessor[E EventConstraint] struct {
	options    eventxOptions
	repo       Repository[E]
	coreChan   chan<- []E
	signalChan chan struct{}

	lastSequence uint64
	retryTimer   Timer

	setSequence func(event *E, sequence uint64)
}

func newDBProcessor[E EventConstraint](
	repo Repository[E],
	coreChan chan<- []E,
	setSequence func(event *E, sequence uint64),
	options eventxOptions,
) *dbProcessor[E] {
	return &dbProcessor[E]{
		options:    options,
		repo:       repo,
		signalChan: make(chan struct{}, 1024),
		coreChan:   coreChan,

		setSequence: setSequence,
	}
}

func (p *dbProcessor[E]) init(ctx context.Context) error {
	p.lastSequence = 0
	p.retryTimer = newTimer(p.options.dbProcessorRetryTimer)

	events, err := p.repo.GetLastEvents(ctx, p.options.getLastEventsLimit)
	if err != nil {
		return err
	}

	if len(events) > 0 {
		p.lastSequence = events[len(events)-1].GetSequence()
		p.coreChan <- events
	}

	return nil
}

func (p *dbProcessor[E]) doSignal() {
	select {
	case p.signalChan <- struct{}{}:
	default:
	}
}

func (p *dbProcessor[E]) handleSignalLoop(ctx context.Context) error {
	for {
		continued, err := p.handleSignal(ctx)
		if err != nil {
			return err
		}
		if !continued {
			return nil
		}
	}
}

func (p *dbProcessor[E]) handleSignal(ctx context.Context) (bool, error) {
	continued := false

	events, err := p.repo.GetUnprocessedEvents(ctx, p.options.getUnprocessedEventsLimit)
	if err != nil {
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}
	if len(events) >= int(p.options.getUnprocessedEventsLimit) {
		continued = true
	}

	for i := range events {
		p.lastSequence++
		p.setSequence(&events[i], p.lastSequence)
	}

	err = p.repo.UpdateSequences(ctx, events)
	if err != nil {
		return false, err
	}

	p.coreChan <- events
	return continued, nil
}

func (p *dbProcessor[E]) runProcessor(ctx context.Context) error {
	select {
	case <-p.signalChan:
		p.retryTimer.Reset()
	BatchLoop:
		for {
			select {
			case <-p.signalChan:
				continue
			default:
				break BatchLoop
			}
		}
		return p.handleSignalLoop(ctx)

	case <-p.retryTimer.Chan():
		p.retryTimer.ResetAfterChan()
		return p.handleSignalLoop(ctx)

	case <-ctx.Done():
		return nil
	}
}
