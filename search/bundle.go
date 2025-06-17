package search

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"golang.org/x/sync/errgroup"
)

type GlobalAggregatesMeta = AggregatesMeta

type BundleResponse struct {
	Searches map[string]*SearchResponse `json:"searches,omitempty"`
	Meta     *GlobalAggregatesMeta      `json:"meta,omitempty"`
}

type QueryBundle struct {
	Transactions   []TxQueryGroup       `json:"transactions,omitempty"`
	ParallelGroups [][]OverallAggregate `json:"parallel_groups,omitempty"`
	QueryGroup
}

func (qb *QueryBundle) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*BundleResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	totalAggregates, totalSearches, err := qb.ValidateAndPreprocess(cfg)
	if err != nil {
		return nil, err
	}

	txGroups, scalarGroups, err := qb.PrepareGroups(cfg, graph)
	if err != nil {
		return nil, err
	}

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(cfg.MaxParallelWorkersPerRequest)

	txGroupsRes := IterRunJobs(
		wgctx, txGroups, wg, cfg.JobTimeout, client,
	)

	scalarRes := IterRunJobs(
		wgctx, scalarGroups, wg, cfg.JobTimeout, client,
	)

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

func (rb *QueryBundle) PrepareGroups(cfg *Config, graph Graph) (
	txGroups []JobifiableTxGroup,
	scalarGroups []JobifiableScalarGroup,
	err error,
) {
	// Prepare standalone searches and aggregates
	searches, aggregates, err := rb.QueryGroup.Prepare(cfg, graph)
	if err != nil {
		return nil, nil, err
	}

	// Build transaction groups: individual transactions + standalone searches
	txCount := len(rb.Transactions)
	searchCount := len(searches)

	if totalTx := txCount + searchCount; totalTx > 0 {
		txGroups = make([]JobifiableTxGroup, totalTx)
		for i, tx := range rb.Transactions {
			grp, err := tx.PrepareTxGroup(cfg, graph)
			if err != nil {
				return nil, nil, err
			}
			txGroups[i] = grp
		}
		for i, cs := range searches {
			txGroups[txCount+i] = &TxGroup{
				IsolationLevel: cfg.Transaction.IsolationLevel,
				Searches:       []*NamedQueryBuild{cs},
			}
		}
	}

	// Build scalar (aggregate) groups: parallel groups + standalone aggregates
	parallelCount := len(rb.ParallelGroups)
	aggCount := len(aggregates)
	chunkSize := cfg.ScalarQueriesChunkSize
	chunkCount := (aggCount + chunkSize - 1) / chunkSize

	if totalScalar := parallelCount + chunkCount; totalScalar > 0 {
		scalarGroups = make([]JobifiableScalarGroup, totalScalar)
		for i, grp := range rb.ParallelGroups {
			scs := make(ScalarGroup, len(grp))
			for j, a := range grp {
				s, err := a.PrepareScalar(graph)
				if err != nil {
					return nil, nil, err
				}
				scs[j] = s
			}
			scalarGroups[i] = scs
		}

		if aggCount > 0 {
			chunks := splitInChunks(aggregates, chunkSize)
			for k, ch := range chunks {
				scalarGroups[parallelCount+k] = ch
			}
		}
	}

	return txGroups, scalarGroups, nil
}

func (r *QueryBundle) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if len(r.Transactions) > 0 && !c.Transaction.EnableClientGroupsInput {
		return 0, 0, &ValidationError{
			Rule: "TxGroupsInputDisable",
			Err:  errors.New("transactions groups usage is not allowed"),
		}
	}

	totalAgg, totalSearch := 0, 0

	agg, search, err := r.QueryGroup.ValidateAndPreprocess(c)
	if err != nil {
		return 0, 0, err
	}
	totalAgg += agg
	totalSearch += search

	for i := range r.Transactions {
		agg, search, err := r.Transactions[i].ValidateAndPreprocess(c)
		if err != nil {
			return 0, 0, err
		}
		totalAgg += agg
		totalSearch += search
	}

	for i1 := range r.ParallelGroups {
		for i2 := range r.ParallelGroups[i1] {
			if err = r.ParallelGroups[i1][i2].ValidateAndPreprocess(&c.FilterConfig); err != nil {
				return 0, 0, err
			}
			totalAgg++
		}
	}

	if c.MaxAggregatesPerRequest != 0 && totalAgg > c.MaxAggregatesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxAggregatesPerBundle",
			Err:  fmt.Errorf("found %d aggregates in bundle, but the maximum allowed is %d", totalAgg, c.MaxAggregatesPerRequest),
		}
	}
	if c.MaxSearchesPerRequest != 0 && totalSearch > c.MaxSearchesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxSearchesPerBundle",
			Err:  fmt.Errorf("found %d searches in bundle, but the maximum allowed is %d", totalSearch, c.MaxSearchesPerRequest),
		}
	}

	return totalAgg, totalSearch, nil
}
