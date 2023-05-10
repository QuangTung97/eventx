package eventx

import (
	"context"
	"database/sql"
	"time"
)

// RetentionJob ...
type RetentionJob[E EventConstraint] struct {
	options retentionOptions

	minSequence  sql.NullInt64
	nextSequence uint64

	retentionRepo RetentionRepository

	runner *Runner[E]
	sub    *Subscriber[E]

	sleepFunc func(ctx context.Context, d time.Duration)
}

// NewRetentionJob ...
func NewRetentionJob[E EventConstraint](
	runner *Runner[E],
	repo RetentionRepository,
	options ...RetentionOption,
) (*RetentionJob[E], error) {
	opts := computeRetentionOptions(options...)

	return &RetentionJob[E]{
		options:       opts,
		retentionRepo: repo,
		runner:        runner,
		sleepFunc: func(ctx context.Context, d time.Duration) {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		},
	}, nil
}

func (j *RetentionJob[E]) logError(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	j.options.errorLogger(err)
}

func (j *RetentionJob[E]) initJob(ctx context.Context) error {
	events, err := j.runner.repo.GetLastEvents(ctx, 1)
	if err != nil {
		return err
	}

	fromSequence := uint64(1)
	if len(events) > 0 {
		fromSequence = events[0].GetSequence() + 1
	}

	minSequence, err := j.retentionRepo.GetMinSequence(ctx)
	if err != nil {
		return err
	}

	j.minSequence = minSequence
	j.nextSequence = fromSequence

	j.sub = j.runner.NewSubscriber(fromSequence, j.options.fetchLimit)
	return nil
}

// RunJob will stop when the context object is cancelled / deadline exceeded
func (j *RetentionJob[E]) RunJob(ctx context.Context) {
	for {
		j.runInLoop(ctx)

		j.sleepFunc(ctx, j.options.errorRetryDuration)

		if ctx.Err() != nil {
			return
		}
	}
}

func (j *RetentionJob[E]) runInLoop(ctx context.Context) {
	err := j.initJob(ctx)
	if err != nil {
		j.logError(ctx, err)
		return
	}

	for {
		for {
			if !j.minSequence.Valid {
				break
			}

			if j.nextSequence-uint64(j.minSequence.Int64) < j.options.maxTotalEvents+j.options.deleteBatchSize {
				break
			}

			beforeSeq := uint64(j.minSequence.Int64) + j.options.deleteBatchSize

			err := j.retentionRepo.DeleteEventsBefore(ctx, beforeSeq)
			if err != nil {
				j.logError(ctx, err)
				return
			}
			j.minSequence = sql.NullInt64{
				Valid: true,
				Int64: int64(beforeSeq),
			}
		}

		events, err := j.sub.Fetch(ctx)
		if err != nil {
			j.logError(ctx, err)
			return
		}
		j.nextSequence = events[len(events)-1].GetSequence() + 1
	}
}
