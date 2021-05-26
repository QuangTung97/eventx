package eventx

import "context"

type coreEvents []Event

type dbProcessor struct {
	options    eventxOptions
	repo       Repository
	coreChan   chan<- coreEvents
	signalChan chan struct{}

	lastSequence uint64
	retryTimer   Timer
}

func newDBProcessor(repo Repository, coreChan chan<- coreEvents, options eventxOptions) *dbProcessor {
	return &dbProcessor{
		options:    options,
		repo:       repo,
		signalChan: make(chan struct{}, 1024),
		coreChan:   coreChan,

		lastSequence: 0,
		retryTimer:   newTimer(options.dbProcessorRetryTimer),
	}
}

func (p *dbProcessor) init(ctx context.Context) error {
	events, err := p.repo.GetLastEvents(ctx, p.options.getLastEventsLimit)
	if err != nil {
		return err
	}

	if len(events) > 0 {
		p.lastSequence = events[len(events)-1].Seq
		p.coreChan <- events
	}

	return nil
}

func (p *dbProcessor) signal() {
	p.signalChan <- struct{}{}
}

func (p *dbProcessor) handleSignal(ctx context.Context) error {
	events, err := p.repo.GetUnprocessedEvents(ctx, p.options.getUnprocessedEventsLimit)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	for i := range events {
		p.lastSequence++
		events[i].Seq = p.lastSequence
	}

	err = p.repo.UpdateSequences(ctx, events)
	if err != nil {
		return err
	}

	p.coreChan <- events
	return nil
}

func (p *dbProcessor) run(ctx context.Context) error {
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
		return p.handleSignal(ctx)

	case <-p.retryTimer.Chan():
		p.retryTimer.ResetAfterChan()
		return p.handleSignal(ctx)

	case <-ctx.Done():
		return nil
	}
}
