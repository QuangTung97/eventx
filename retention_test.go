package eventx

import (
	"context"
	"database/sql"
	"fmt"
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

func (r *retentionTest) initJob(options ...RetentionOption) error {
	job, err := NewRetentionJob(r.runner, r.retentionRepo, options...)
	r.job = job
	return err
}

func (r *retentionTest) mustInit(maxSize uint64) {
	err := r.initJob(
		WithMaxTotalEvents(maxSize),
		WithRetentionErrorLogger(func(err error) {
			fmt.Println("ERROR retention:", err)
		}),
		WithRetentionErrorRetryDuration(45*time.Second),
	)
	if err != nil {
		panic(err)
	}
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

func TestRetentionJob(t *testing.T) {
	t.Run("new-call-repo", func(t *testing.T) {
		r := newRetentionTest()

		err := r.initJob()
		assert.Equal(t, nil, err)

		assert.Equal(t, 1, len(r.repo.GetLastEventsCalls()))
		assert.Equal(t, uint64(1), r.repo.GetLastEventsCalls()[0].Limit)

		assert.Equal(t, 1, len(r.retentionRepo.GetMinSequenceCalls()))
	})

	t.Run("run-delete-immediately", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 13, seq: 13},
		})
		r.stubGetMinSeq(9) // 13 - 9 + 1 = 5 > 4

		r.mustInit(4)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 1, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(10), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
	})

	t.Run("run-delete-multiple-times--immediately", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 17, seq: 17},
		})
		r.stubGetMinSeq(9) // 13 - 9 + 1 = 5 > 4

		r.mustInit(4)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 2, len(r.retentionRepo.DeleteEventsBeforeCalls()))
		assert.Equal(t, uint64(13), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
		assert.Equal(t, uint64(14), r.retentionRepo.DeleteEventsBeforeCalls()[1].BeforeSeq)
	})

	t.Run("run--not-delete", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 17, seq: 17},
		})
		r.stubGetMinSeq(13) // 17 - 13 + 1 = 5

		r.mustInit(5)

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
			{id: 17, seq: 17},
		})
		r.stubGetMinSeqNull()

		r.mustInit(5)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.doRun()
		time.Sleep(10 * time.Millisecond)
		r.waitFinish()

		assert.Equal(t, 0, len(r.retentionRepo.DeleteEventsBeforeCalls()))
	})

	t.Run("run--delete-after-notify-events", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 17, seq: 17},
		})
		r.stubGetMinSeq(13)

		r.mustInit(5)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.stubGetLastEvents([]testEvent{
			{id: 13, seq: 13},
			{id: 14, seq: 14},
			{id: 15, seq: 15},
			{id: 16, seq: 16},
			{id: 17, seq: 17},
		})

		r.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return []testEvent{
				{id: 18},
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
		assert.Equal(t, uint64(14), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
	})

	t.Run("run--delete-multiple-times--after-notify-events", func(t *testing.T) {
		r := newRetentionTest()

		r.stubGetLastEvents([]testEvent{
			{id: 17, seq: 17},
		})
		r.stubGetMinSeq(15)

		r.mustInit(3)

		r.retentionRepo.DeleteEventsBeforeFunc = func(ctx context.Context, beforeSeq uint64) error {
			return nil
		}

		r.stubGetLastEvents([]testEvent{
			{id: 15, seq: 15},
			{id: 16, seq: 16},
			{id: 17, seq: 17},
		})

		r.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]testEvent, error) {
			return []testEvent{
				{id: 18}, {id: 19},
				{id: 20}, {id: 21},
				{id: 22},
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
		assert.Equal(t, uint64(18), r.retentionRepo.DeleteEventsBeforeCalls()[0].BeforeSeq)
		assert.Equal(t, uint64(20), r.retentionRepo.DeleteEventsBeforeCalls()[1].BeforeSeq)
	})
}
