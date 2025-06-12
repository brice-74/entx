package search

import (
	"context"
	"maps"
	"time"

	"golang.org/x/sync/errgroup"
)

type Composite struct {
	graph  Graph
	client Client
	cfg    *Config
}

func NewComposite(
	graph Graph,
	client Client,
	cfg *Config,
) *Composite {
	return &Composite{
		graph,
		client,
		cfg,
	}
}

func (c *Composite) Exec(ctx context.Context, req *RequestBundle) (*CompositeResponse, error) {
	ctx, cancel := contextTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()

	totalAggregates, totalSearches, err := req.ValidateAndPreprocess(c.cfg)
	if err != nil {
		return nil, err
	}

	txGroups, scalarGroups, err := req.PrepareGroups(c.cfg, c.graph)
	if err != nil {
		return nil, err
	}

	var (
		txGroupsRes []*TransactionGroupResponse
		scalarRes   []map[string]any
	)

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(c.cfg.MaxParallelWorkersPerRequest)

	if lenght := len(txGroups); lenght > 0 {
		var jobs = make([]TransactionJob, 0, lenght)
		for i, g := range txGroups {
			jobs[i] = g.ToJob(c.client)
		}

		txGroupsRes = RunJobs(wgctx, jobs, wg, 5*time.Second)
	}

	if lenght := len(scalarGroups); lenght > 0 {
		var jobs = make([]ScalarJob, 0, lenght)
		for i, g := range scalarGroups {
			jobs[i] = g.ToJob(c.client)
		}

		scalarRes = RunJobs(wgctx, jobs, wg, 5*time.Second)
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	var (
		searchesRes   = make(map[string]*SearchResponse, totalSearches)
		aggregatesRes = make(map[string]any, totalAggregates)
	)
	for _, res := range txGroupsRes {
		maps.Copy(searchesRes, res.Searches)
		maps.Copy(aggregatesRes, res.Aggregates)
	}

	for _, res := range scalarRes {
		maps.Copy(aggregatesRes, res)
	}

	return &CompositeResponse{Searches: searchesRes, Meta: GlobalAggregatesMeta{aggregatesRes}}, nil
}

type Jobifiable[R any] interface {
	ToJob(Client) Job[R]
}

func iterRun[R any, J Jobifiable[R]](js []J) R {
	if lenght := len(js); lenght > 0 {
		var jobs = make([]Job[R], 0, lenght)
		for i, j := range js {
			jobs[i] = j.ToJob()
		}

		return RunJobs(wgctx, jobs, wg, 5*time.Second)
	}
	var zero R
	return zero
}
