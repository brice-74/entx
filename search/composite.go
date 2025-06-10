package search

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Composite struct {
	graph  Graph
	client Client
	conf   *Config
}

func NewComposite(
	graph Graph,
	client Client,
	conf *Config,
) *Composite {
	return &Composite{
		graph,
		client,
		conf,
	}
}

type TransactionGroup struct {
	client   Client
	tx       Transaction
	searches []*CompositeSearch
}

func (c *Composite) Exec(ctx context.Context, req *RequestBundle) (*CompositeResponse, error) {
	ctx, cancel := contextTimeout(ctx, c.conf.RequestTimeout)
	defer cancel()

	totalAggregates, totalSearches, err := req.ValidateAndPreprocess(c.conf)
	if err != nil {
		return nil, err
	}

	var scalars = make([]*ScalarQuery, 0, len(req.Aggregates))
	for i, a := range req.Aggregates {
		composite, err := a.PrepareComposite(c.graph)
		if err != nil {
			return nil, err
		}
		scalars[i] = composite.ToScalarQuery()
	}

	for i, s := range req.Searches {
		composite, err := s.PrepareComposite(i, c.conf, c.graph, c.client)
		if err != nil {
			return nil, err
		}
		scalars[i] = composite.ToScalarQuery()
	}

	/* txGroups := make([]*TransactionGroup, 0, len(req.Transactions))
	for i, t := range req.Transactions {
		isolationLevel := c.conf.Transaction.IsolationLevel
		if v := t.TransactionIsolationLevel; v != nil {
			isolationLevel = sql.IsolationLevel(*v)
		}
		tx, clientTx, err := c.client.Tx(ctx, isolationLevel)
		if err != nil {
			return nil, err
		}
		txGroup := TransactionGroup{
			tx:       tx,
			client:   clientTx,
			searches: make([]*CompositeSearch, 0, len(req.Searches)),
		}

		for _, s := range req.Searches {
			composite, err := s.PrepareComposite(i, c.conf, c.graph, c.client)
			if err != nil {
				return nil, err
			}

			txGroup.searches = append(txGroup.searches, composite)
		}
	} */

	return &CompositeResponse{Searches: searches, Meta: GlobalAggregatesMeta{aggregates}}, nil
}

func (c *Composite) searchJob() {

}

type Job[K comparable, R any] struct {
	Key  K
	Exec func(ctx context.Context) (R, error)
}

func RunJobs[K comparable, R any](
	ctx context.Context,
	jobs []Job[K, R],
	wg *errgroup.Group,
	timeout time.Duration,
) map[K]R {
	results := make(map[K]R, len(jobs))
	var mu sync.Mutex

	for _, job := range jobs {
		job := job
		if ctx.Err() != nil {
			break
		}

		wg.Go(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			res, err := job.Exec(ctx)
			if err != nil {
				return err
			}

			mu.Lock()
			results[job.Key] = res
			mu.Unlock()
			return nil
		})
	}

	return results
}
