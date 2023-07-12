package eventx

import (
	"context"
	"time"
)

// RetentionJob ...
type RetentionJob[E EventConstraint] struct {
	options retentionOptions

	minSequence  uint64
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
) *RetentionJob[E] {
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
	}
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

	j.minSequence = 1
	if minSequence.Valid {
		j.minSequence = uint64(minSequence.Int64)
	}
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
			if j.nextSequence < j.options.maxTotalEvents+j.options.deleteBatchSize+j.minSequence {
				break
			}

			beforeSeq := j.minSequence + j.options.deleteBatchSize

			err := j.retentionRepo.DeleteEventsBefore(ctx, beforeSeq)
			if err != nil {
				j.logError(ctx, err)
				return
			}
			j.minSequence = beforeSeq
		}

		events, err := j.sub.Fetch(ctx)
		if err != nil {
			j.logError(ctx, err)
			return
		}
		j.nextSequence = events[len(events)-1].GetSequence() + 1
	}
}
