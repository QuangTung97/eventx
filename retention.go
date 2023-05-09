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

	sub *Subscriber[E]

	sleepFunc func(ctx context.Context, d time.Duration)
}

// NewRetentionJob ...
func NewRetentionJob[E EventConstraint](
	runner *Runner[E],
	repo RetentionRepository,
	options ...RetentionOption,
) (*RetentionJob[E], error) {
	events, err := runner.repo.GetLastEvents(context.Background(), 1)
	if err != nil {
		return nil, err
	}

	fromSequence := uint64(1)
	if len(events) > 0 {
		fromSequence = events[0].GetSequence() + 1
	}

	minSequence, err := repo.GetMinSequence(context.Background())
	if err != nil {
		return nil, err
	}

	opts := computeRetentionOptions(options...)

	sub := runner.NewSubscriber(fromSequence, opts.fetchLimit)

	return &RetentionJob[E]{
		options: opts,

		minSequence:  minSequence,
		nextSequence: fromSequence,

		retentionRepo: repo,

		sub: sub,

		sleepFunc: func(ctx context.Context, d time.Duration) {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		},
	}, nil
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
	for {
		for j.minSequence.Valid && j.nextSequence-uint64(j.minSequence.Int64) > j.options.maxTotalEvents {
			deleteBefore := j.nextSequence - j.options.maxTotalEvents

			beforeSeq := uint64(j.minSequence.Int64) + j.options.maxTotalEvents
			if beforeSeq > deleteBefore {
				beforeSeq = deleteBefore
			}

			err := j.retentionRepo.DeleteEventsBefore(ctx, beforeSeq)
			if err != nil {
				j.options.errorLogger(err) // TODO testing
				return
			}
			j.minSequence = sql.NullInt64{
				Valid: true,
				Int64: int64(beforeSeq),
			}
		}

		events, err := j.sub.Fetch(ctx)
		if err != nil {
			j.options.errorLogger(err) // TODO testing
			return
		}
		if len(events) == 0 {
			return
		}
		j.nextSequence = events[len(events)-1].GetSequence() + 1
	}
}
