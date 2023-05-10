package eventx

import (
	"context"
	"database/sql"
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

type retentionTest struct {
	repo          *RepositoryMock[testEvent]
	retentionRepo *RetentionRepositoryMock
	runner        *Runner[testEvent]

	job *RetentionJob[testEvent]

	wg     sync.WaitGroup
	cancel func()
}

func newRetentionTest() *retentionTest {
	repo := &RepositoryMock[testEvent]{}
	retentionRepo := &RetentionRepositoryMock{}

	runner := NewRunner[testEvent](repo, setTestEventSeq)

	r := &retentionTest{
		repo:          repo,
		retentionRepo: retentionRepo,
		runner:        runner,
	}

	r.stubGetLastEvents([]testEvent{
		{id: 1000, seq: 1000},
	})
	r.stubGetMinSeqNull()

	return r
}

func (r *retentionTest) initJob(options ...RetentionOption) {
	job := NewRetentionJob(r.runner, r.retentionRepo, options...)
	r.job = job
}

func (r *retentionTest) mustInit(maxSize uint64, batchSize uint64, options ...RetentionOption) {
	opts := []RetentionOption{
		WithMaxTotalEvents(maxSize),
		WithDeleteBatchSize(batchSize),
	}
	opts = append(opts, options...)

	r.initJob(opts...)
}

func (r *retentionTest) doRun() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.wg.Add(2)
	go func() {
		defer r.wg.Done()

		r.runner.Run(ctx)
	}()

	go func() {
		defer r.wg.Done()

		r.job.RunJob(ctx)
	}()
}

func (r *retentionTest) waitFinish() {
	r.cancel()
	r.wg.Wait()
}

func (r *retentionTest) stubGetLastEvents(events []testEvent) {
	r.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
		return events, nil
	}
}

func (r *retentionTest) stubGetMinSeq(seq uint64) {
	r.retentionRepo.GetMinSequenceFunc = func(ctx context.Context) (sql.NullInt64, error) {
		return sql.NullInt64{
			Valid: true,
			Int64: int64(seq),
		}, nil
	}
}

func (r *retentionTest) stubGetMinSeqNull() {
	r.retentionRepo.GetMinSequenceFunc = func(ctx context.Context) (sql.NullInt64, error) {
		return sql.NullInt64{}, nil
	}
}

//revive:disable-next-line:cognitive-complexity
func TestRetentionJob(t *testing.T) {
	t.Run("new-call-repo", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 43, seq: 14},
		})
		r.stubGetMinSeq(9) // 14 - 9 + 1 = 6 = 4 + 2

		r.initJob()

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 2, len(r.repo.GetLastEventsCalls()))
		assert.Equal(t, uint64(1), r.repo.GetLastEventsCalls()[0].Limit)

		assert.Equal(t, 1, len(r.retentionRepo.GetMinSequenceCalls()))
	})

	t.Run("run-delete-immediately", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 43, seq: 14},
		})
		r.stubGetMinSeq(9) // 14 - 9 + 1 = 6 = 4 + 2

		r.mustInit(4, 2)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 1, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(9+2), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
	})

	t.Run("run-delete-multiple-times--immediately", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 50, seq: 17},
		})
		r.stubGetMinSeq(9) // 17 - 9 + 1 = 9 = 4 + 2 + 2 + 1

		r.mustInit(4, 2)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 2, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(9+2), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
		assert.Equal(t, uint64(9+2+2), r.retentionRepo.DeleteEventsBeforeCalls()[1].BeforeSeq)
	})

	t.Run("run--not-delete", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 47, seq: 18},
		})
		r.stubGetMinSeq(13) // 18 - 13 + 1 = 6 = 5 + 1

		r.mustInit(5, 2)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 0, len(r.retentionRepo.DeleteEventsBeforeCalls()))
	})

	t.Run("run--not-delete--when-min-seq-is-null", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 47, seq: 17},
		})
		r.stubGetMinSeqNull()

		var logErr error
		r.mustInit(5, 3, WithRetentionErrorLogger(func(err error) {
			logErr = err
		}))

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 0, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, nil, logErr)
	})

	t.Run("run--delete-after-notify-events", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 47, seq: 19},
		})
		r.stubGetMinSeq(13) // 19 - 13 + 1 = 7 = 5 + 2

		r.mustInit(5, 3)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.stubGetLastEvents([]testEvent{
			{id: 43, seq: 13},
			{id: 44, seq: 14},
			{id: 45, seq: 15},
			{id: 46, seq: 16},
			{id: 47, seq: 17},
			{id: 48, seq: 18},
			{id: 49, seq: 19},
		})

		r.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return []testEvent{
				{id: 50},
			}, nil
		}
		r.repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
			return nil
		}

		r.doRun()

		r.runner.Signal()

		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 1, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(13+3), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
	})

	t.Run("run--delete-multiple-times--after-notify-events", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 48, seq: 18},
		})
		r.stubGetMinSeq(15) // 18 - 15 + 1 = 4 = 3 + 1

		r.mustInit(3, 2)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.stubGetLastEvents([]testEvent{
			{id: 45, seq: 15},
			{id: 46, seq: 16},
			{id: 47, seq: 17},
			{id: 48, seq: 18},
		})

		r.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return []testEvent{
				{id: 49}, {id: 50},
				{id: 51}, {id: 52},
			}, nil
		}
		r.repo.UpdateSequencesFunc = func(ctx context.Context, events []testEvent) error {
			return nil
		}

		r.doRun()

		r.runner.Signal()

		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 2, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(15+2), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
		assert.Equal(t, uint64(15+2+2), r.retentionRepo.DeleteEventsBeforeCalls()[1].BeforeSeq)
	})

	t.Run("run--init-get-last-events-returns-error--do-sleep", func(t *testing.T) {
		r := newRetentionTest()

		getErr := errors.New("get error")
		r.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return nil, getErr
		}

		var logErr error
		r.mustInit(4, 2,
			WithRetentionErrorLogger(func(err error) {
				logErr = err
			}),
			WithRetentionErrorRetryDuration(50*time.Millisecond),
		)

		var duration time.Duration

		prevSleepFunc := r.job.sleepFunc
		r.job.sleepFunc = func(ctx context.Context, d time.Duration) {
			duration = d
			prevSleepFunc(ctx, d)
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 2, len(r.repo.GetLastEventsCalls()))

		assert.Equal(t, getErr, logErr)
		assert.Equal(t, 50*time.Millisecond, duration)
	})

	t.Run("run--init-get-last-events-returns-error--do-sleep--sleep-finish-call-again", func(t *testing.T) {
		r := newRetentionTest()

		getErr := errors.New("get error")
		r.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return nil, getErr
		}

		var logErr error
		r.mustInit(4, 2,
			WithRetentionErrorLogger(func(err error) {
				logErr = err
			}),
			WithRetentionErrorRetryDuration(50*time.Millisecond),
		)

		var duration time.Duration

		prevSleepFunc := r.job.sleepFunc
		r.job.sleepFunc = func(ctx context.Context, d time.Duration) {
			duration = d
			prevSleepFunc(ctx, d)
		}

		r.doRun()
		time.Sleep(70 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 3, len(r.repo.GetLastEventsCalls()))

		assert.Equal(t, getErr, logErr)
		assert.Equal(t, 50*time.Millisecond, duration)
	})

	t.Run("run--init-get-min-sequence-error", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 47, seq: 17},
		})

		getErr := errors.New("get error")
		r.retentionRepo.GetMinSequenceFunc = func(ctx context.Context) (sql.NullInt64, error) {
			return sql.NullInt64{}, getErr
		}

		var logErr error
		r.mustInit(4, 2,
			WithRetentionErrorLogger(func(err error) {
				logErr = err
				defaultRetentionErrorLogger(err)
			}),
		)

		var duration time.Duration

		prevSleepFunc := r.job.sleepFunc
		r.job.sleepFunc = func(ctx context.Context, d time.Duration) {
			duration = d
			prevSleepFunc(ctx, d)
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 2, len(r.repo.GetLastEventsCalls()))

		assert.Equal(t, getErr, logErr)
		assert.Equal(t, 30*time.Second, duration)
	})

	t.Run("run--delete-immediately-with-error", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 43, seq: 14},
		})
		r.stubGetMinSeq(9) // 14 - 9 + 1 = 6 = 4 + 2

		var logErr error
		r.mustInit(4, 2,
			WithRetentionErrorLogger(func(err error) {
				logErr = err
			}),
		)

		deleteErr := errors.New("delete error")
		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return deleteErr
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 1, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(9+2), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)

		assert.Equal(t, deleteErr, logErr)
	})
}
