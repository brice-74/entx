package search

import (
	"context"
	"maps"

	"golang.org/x/sync/errgroup"
)

type Executor struct {
	graph  Graph
	client Client
	cfg    *Config
}

func NewExecutor(
	graph Graph,
	client Client,
	cfg *Config,
) *Executor {
	return &Executor{
		graph,
		client,
		cfg,
	}
}

func (c *Executor) QueryBundle(ctx context.Context, req QueryBundleInterface) (*BundleResponse, error) {
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
		txGroupsRes []*TxGroupJobResponse
		scalarRes   []map[string]any
	)

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(c.cfg.MaxParallelWorkersPerRequest)

	if lenght := len(txGroups); lenght > 0 {
		var jobs = make([]TxGroupJob, 0, lenght)
		for i, g := range txGroups {
			jobs[i] = g.ToJob(c.client)
		}

		txGroupsRes = RunJobs(wgctx, jobs, wg, c.cfg.JobTimeout)
	}

	if lenght := len(scalarGroups); lenght > 0 {
		var jobs = make([]ScalarGroupJob, 0, lenght)
		for i, g := range scalarGroups {
			jobs[i] = g.ToJob(c.client)
		}

		scalarRes = RunJobs(wgctx, jobs, wg, c.cfg.JobTimeout)
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	bundleRes := BundleResponse{}

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

	if len(searchesRes) > 0 {
		bundleRes.Searches = searchesRes
	}

	if len(aggregatesRes) > 0 {
		bundleRes.Meta = &GlobalAggregatesMeta{aggregatesRes}
	}

	return &bundleRes, nil
}
