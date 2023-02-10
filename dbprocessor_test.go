package eventx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func initWithEvents(repo *RepositoryMock[testEvent], p *dbProcessor[testEvent], events []testEvent) {
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return events, nil
	}
	_ = p.init(context.Background())
}

func drainCoreEventsChan(ch <-chan []testEvent) []testEvent {
	var events []testEvent
	for {
		select {
		case e := <-ch:
			events = append(events, e...)
		default:
			return events
		}
	}
}

func newDBProcessorWithRepo(repo Repository[testEvent]) *dbProcessor[testEvent] {
	coreChan := make(chan []testEvent, 1024)
	return newDBProcessor[testEvent](repo, coreChan, setTestEventSeq, computeOptions())
}

func newDBProcessorWithRepoAndCoreChan(
	repo Repository[testEvent], coreChan chan<- []testEvent) *dbProcessor[testEvent] {
	return newDBProcessor[testEvent](repo, coreChan, setTestEventSeq, computeOptions())
}

func newDBProcessorWithOptions(
	repo Repository[testEvent], coreChan chan<- []testEvent, options eventxOptions,
) *dbProcessor[testEvent] {
	return newDBProcessor[testEvent](repo, coreChan, setTestEventSeq, options)
}

func TestDBProcessor_Init_EmptyLastEvents(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	p := newDBProcessorWithRepo(repo)

	var callLimit uint64
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		callLimit = limit
		return nil, nil
	}

	ctx := context.Background()
	err := p.init(ctx)

	assert.Equal(t, 1, len(repo.GetLastEventsCalls()))
	assert.Equal(t, uint64(256), callLimit)
	assert.Equal(t, nil, err)
}

func TestDBProcessor_Init_Call_GetLastEvents_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	p := newDBProcessorWithRepo(repo)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return nil, errors.New("get-last-events-error")
	}

	ctx := context.Background()
	err := p.init(ctx)

	assert.Equal(t, 1, len(repo.GetLastEventsCalls()))
	assert.Equal(t, errors.New("get-last-events-error"), err)
}

func TestDBProcessor_Init_CoreChan_Events(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)

	events := []testEvent{
		{id: 10, seq: 100},
		{id: 18, seq: 101},
		{id: 15, seq: 102},
	}
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return events, nil
	}
	_ = p.init(context.Background())

	assert.Equal(t, 1, len(coreChan))
	coreEvents := drainCoreEventsChan(coreChan)
	assert.Equal(t, events, coreEvents)
}

func TestDBProcessor_Run_Context_Cancel(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	p := newDBProcessorWithRepo(repo)

	initWithEvents(repo, p, nil)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := p.runProcessor(ctx)
	assert.Equal(t, nil, err)
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	p := newDBProcessorWithRepo(repo)
	initWithEvents(repo, p, nil)

	timer := &TimerMock{}
	p.retryTimer = timer

	timer.ResetFunc = func() {
	}
	timer.ChanFunc = func() <-chan time.Time {
		return nil
	}

	var callLimit uint64
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		callLimit = limit
		return nil, errors.New("get-unprocessed-error")
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, 1, len(timer.ResetCalls()))
	assert.Equal(t, uint64(256), callLimit)
	assert.Equal(t, errors.New("get-unprocessed-error"), err)
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Empty(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	p := newDBProcessorWithRepo(repo)

	initWithEvents(repo, p, nil)

	var callLimit uint64
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		callLimit = limit
		return nil, nil
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, uint64(256), callLimit)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.GetUnprocessedEventsCalls()))
	assert.Equal(t, 0, len(repo.UpdateSequencesCalls()))
}

//revive:disable-next-line:get-return
func getUnprocessedWithEvents(repo *RepositoryMock[testEvent], events []testEvent) {
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return events, nil
	}
}

func TestDBProcessor_Signal_GetUnprocessedEvents_WithEvents_Update_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	p := newDBProcessorWithRepo(repo)
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
	})

	var updateEvents []testEvent
	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		updateEvents = events
		return errors.New("update-seq-error")
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, errors.New("update-seq-error"), err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, []testEvent{
		{id: 10, seq: 1},
		{id: 7, seq: 2},
		{id: 13, seq: 3},
	}, updateEvents)
}

func TestDBProcessor_Signal_GetUnprocessedEvents_WithEvents_Update_OK(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		return nil
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 1, len(coreChan))
	assert.Equal(t, []testEvent{
		{id: 10, seq: 1},
		{id: 7, seq: 2},
		{id: 13, seq: 3},
	}, drainCoreEventsChan(coreChan))
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Reach_Limit__Resend_Signal(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)
	p := newDBProcessorWithOptions(repo, coreChan, computeOptions(
		WithGetUnprocessedEventsLimit(4),
	))
	initWithEvents(repo, p, nil)

	events := []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
		{id: 18},
	}
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		if len(repo.GetUnprocessedEventsCalls()) > 1 {
			return []testEvent{{id: 33}}, nil
		}
		return events, nil
	}

	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		return nil
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, []testEvent{
		{id: 10, seq: 1},
		{id: 7, seq: 2},
		{id: 13, seq: 3},
		{id: 18, seq: 4},
		{id: 33, seq: 5},
	}, drainCoreEventsChan(coreChan))

	assert.Equal(t, 0, len(p.signalChan))
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Near_Reach_Limit__Not_Resend_Signal(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)
	p := newDBProcessorWithOptions(repo, coreChan, computeOptions(
		WithGetUnprocessedEventsLimit(4),
	))
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		return nil
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 1, len(coreChan))
	assert.Equal(t, []testEvent{
		{id: 10, seq: 1},
		{id: 7, seq: 2},
		{id: 13, seq: 3},
	}, drainCoreEventsChan(coreChan))

	assert.Equal(t, 0, len(p.signalChan))
}

func TestDBProcessor_Signal_With_Init_Events(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, []testEvent{
		{id: 5, seq: 50},
		{id: 8, seq: 51},
		{id: 5, seq: 52},
	})

	getUnprocessedWithEvents(repo, []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		return nil
	}

	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, []testEvent{
		{id: 5, seq: 50},
		{id: 8, seq: 51},
		{id: 5, seq: 52},
		{id: 10, seq: 53},
		{id: 7, seq: 54},
		{id: 13, seq: 55},
	}, drainCoreEventsChan(coreChan))
}

func TestDBProcessor_Signal_With_Init_Events_Multiple_Signal(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, []testEvent{
		{id: 5, seq: 50},
		{id: 8, seq: 51},
		{id: 5, seq: 52},
	})

	getUnprocessedWithEvents(repo, []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		return nil
	}

	p.doSignal()
	p.doSignal()
	err := p.runProcessor(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, 0, len(p.signalChan))
}

func TestDBProcessor_Run_With_Timeout(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock[testEvent]{}
	coreChan := make(chan []testEvent, 1024)

	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, []testEvent{
		{id: 5, seq: 50},
		{id: 8, seq: 51},
	})

	timer := &TimerMock{}
	p.retryTimer = timer

	timerChan := make(chan time.Time, 1)
	timerChan <- time.Now()

	timer.ChanFunc = func() <-chan time.Time {
		return timerChan
	}
	timer.ResetAfterChanFunc = func() {}

	getUnprocessedWithEvents(repo, []testEvent{
		{id: 10},
		{id: 7},
		{id: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
		return nil
	}

	err := p.runProcessor(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, 0, len(p.signalChan))
	assert.Equal(t, 1, len(timer.ResetAfterChanCalls()))
}
