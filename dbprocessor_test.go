package eventx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func initWithEvents(repo *RepositoryMock, p *dbProcessor, events []Event) {
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}
	_ = p.init(context.Background())
}

func drainCoreEventsChan(ch <-chan coreEvents) []Event {
	var events []Event
	for {
		select {
		case e := <-ch:
			events = append(events, e...)
		default:
			return events
		}
	}
}

func newDBProcessorWithRepo(repo Repository) *dbProcessor {
	coreChan := make(chan coreEvents, 1024)
	return newDBProcessor(repo, coreChan, computeOptions())
}

func newDBProcessorWithRepoAndCoreChan(repo Repository, coreChan chan<- coreEvents) *dbProcessor {
	return newDBProcessor(repo, coreChan, computeOptions())
}

func newDBProcessorWithOptions(repo Repository, coreChan chan<- coreEvents, options eventxOptions) *dbProcessor {
	return newDBProcessor(repo, coreChan, options)
}

func TestDBProcessor_Init_EmptyLastEvents(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	p := newDBProcessorWithRepo(repo)

	var callLimit uint64
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
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

	repo := &RepositoryMock{}
	p := newDBProcessorWithRepo(repo)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, errors.New("get-last-events-error")
	}

	ctx := context.Background()
	err := p.init(ctx)

	assert.Equal(t, 1, len(repo.GetLastEventsCalls()))
	assert.Equal(t, errors.New("get-last-events-error"), err)
}

func TestDBProcessor_Init_CoreChan_Events(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)

	events := []Event{
		{ID: 10, Seq: 100},
		{ID: 18, Seq: 101},
		{ID: 15, Seq: 102},
	}
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}
	_ = p.init(context.Background())

	assert.Equal(t, 1, len(coreChan))
	coreEvents := drainCoreEventsChan(coreChan)
	assert.Equal(t, events, coreEvents)
}

func TestDBProcessor_Run_Context_Cancel(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	p := newDBProcessorWithRepo(repo)

	initWithEvents(repo, p, nil)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := p.run(ctx)
	assert.Equal(t, nil, err)
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
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
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		callLimit = limit
		return nil, errors.New("get-unprocessed-error")
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, 1, len(timer.ResetCalls()))
	assert.Equal(t, uint64(256), callLimit)
	assert.Equal(t, errors.New("get-unprocessed-error"), err)
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Empty(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	p := newDBProcessorWithRepo(repo)

	initWithEvents(repo, p, nil)

	var callLimit uint64
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		callLimit = limit
		return nil, nil
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, uint64(256), callLimit)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.GetUnprocessedEventsCalls()))
	assert.Equal(t, 0, len(repo.UpdateSequencesCalls()))
}

func getUnprocessedWithEvents(repo *RepositoryMock, events []Event) {
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}
}

func TestDBProcessor_Signal_GetUnprocessedEvents_WithEvents_Update_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	p := newDBProcessorWithRepo(repo)
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
	})

	var updateEvents []Event
	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		updateEvents = events
		return errors.New("update-seq-error")
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, errors.New("update-seq-error"), err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, []Event{
		{ID: 10, Seq: 1},
		{ID: 7, Seq: 2},
		{ID: 13, Seq: 3},
	}, updateEvents)
}

func TestDBProcessor_Signal_GetUnprocessedEvents_WithEvents_Update_OK(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 1, len(coreChan))
	assert.Equal(t, []Event{
		{ID: 10, Seq: 1},
		{ID: 7, Seq: 2},
		{ID: 13, Seq: 3},
	}, drainCoreEventsChan(coreChan))
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Reach_Limit__Resend_Signal(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)
	p := newDBProcessorWithOptions(repo, coreChan, computeOptions(
		WithGetUnprocessedEventsLimit(4),
	))
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
		{ID: 18},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 1, len(coreChan))
	assert.Equal(t, []Event{
		{ID: 10, Seq: 1},
		{ID: 7, Seq: 2},
		{ID: 13, Seq: 3},
		{ID: 18, Seq: 4},
	}, drainCoreEventsChan(coreChan))

	assert.Equal(t, 1, len(p.signalChan))
}

func TestDBProcessor_Signal_GetUnprocessedEvents_Near_Reach_Limit__Not_Resend_Signal(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)
	p := newDBProcessorWithOptions(repo, coreChan, computeOptions(
		WithGetUnprocessedEventsLimit(4),
	))
	initWithEvents(repo, p, nil)

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 1, len(coreChan))
	assert.Equal(t, []Event{
		{ID: 10, Seq: 1},
		{ID: 7, Seq: 2},
		{ID: 13, Seq: 3},
	}, drainCoreEventsChan(coreChan))

	assert.Equal(t, 0, len(p.signalChan))
}

func TestDBProcessor_Signal_With_Init_Events(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, []Event{
		{ID: 5, Seq: 50},
		{ID: 8, Seq: 51},
		{ID: 5, Seq: 52},
	})

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, []Event{
		{ID: 5, Seq: 50},
		{ID: 8, Seq: 51},
		{ID: 5, Seq: 52},
		{ID: 10, Seq: 53},
		{ID: 7, Seq: 54},
		{ID: 13, Seq: 55},
	}, drainCoreEventsChan(coreChan))
}

func TestDBProcessor_Signal_With_Init_Events_Multiple_Signal(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)
	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, []Event{
		{ID: 5, Seq: 50},
		{ID: 8, Seq: 51},
		{ID: 5, Seq: 52},
	})

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	p.signal()
	p.signal()
	err := p.run(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, 0, len(p.signalChan))
}

func TestDBProcessor_Run_With_Timeout(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	coreChan := make(chan coreEvents, 1024)

	p := newDBProcessorWithRepoAndCoreChan(repo, coreChan)
	initWithEvents(repo, p, []Event{
		{ID: 5, Seq: 50},
		{ID: 8, Seq: 51},
	})

	timer := &TimerMock{}
	p.retryTimer = timer

	timerChan := make(chan time.Time, 1)
	timerChan <- time.Now()

	timer.ChanFunc = func() <-chan time.Time {
		return timerChan
	}
	timer.ResetAfterChanFunc = func() {}

	getUnprocessedWithEvents(repo, []Event{
		{ID: 10},
		{ID: 7},
		{ID: 13},
	})

	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	err := p.run(context.Background())

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(repo.UpdateSequencesCalls()))
	assert.Equal(t, 2, len(coreChan))
	assert.Equal(t, 0, len(p.signalChan))
	assert.Equal(t, 1, len(timer.ResetAfterChanCalls()))
}
