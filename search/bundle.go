package search

import (
	"database/sql"
	stdsql "database/sql"
	"errors"
	"fmt"
)

type QueryBundle struct {
	Transactions   []TxQueryGroup      `json:"transactions,omitempty"`
	ParallelGroups []OverallAggregates `json:"parallel_aggregates_groups,omitempty"`
	QueryGroup
}

type AggregatesMeta struct {
	Aggregates map[string]any `json:"aggregates,omitempty"`
}

type GroupResponse struct {
	Searches map[string]*SearchResponse `json:"searches,omitempty"`
	Meta     *AggregatesMeta            `json:"meta,omitempty"`
}

/* func (r *QueryBundle) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	countAggregates, countSearches, err := r.ValidateAndPreprocess(cfg)
	if err != nil {
		return nil, err
	}

}

func (r *QueryBundle) Prepare(cfg *Config, graph Graph) (*any, error) {
	qgBuild, err := r.QueryGroup.Prepare(cfg, graph)
	if err != nil {
		return nil, err
	}

	qgBuildCountTx := 0
	for _, build := range qgBuild.searches {
		if build.Paginate != nil && build.EnableTransaction {
			qgBuildCountTx++
		}
	}

	txBuilds := make([]*TxQueryGroupBuild, 0, len(r.Transactions)+qgBuildCountTx)
	for _, build := range qgBuild.searches {
		if build.Paginate != nil && build.EnableTransaction {
			txBuilds
		}
	}

	indexLevel := len(txBuilds)
	for i, t := range r.Transactions {
		b, err := t.Prepare(cfg, graph)
		if err != nil {
			return nil, err
		}
		txBuilds[i] = b
	}

} */

func (r *QueryBundle) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
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

	for i := range r.ParallelGroups {
		count, err := r.ParallelGroups[i].ValidateAndPreprocess(&c.FilterConfig)
		if err != nil {
			return 0, 0, err
		}
		totalAgg += count
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

type TxQueryGroupBuild struct {
	IsolationLevel sql.IsolationLevel
	QueryGroupBuild
}

type TxQueryGroup struct {
	TransactionIsolationLevel *stdsql.IsolationLevel `json:"transaction_isolation_level,omitempty"`
	QueryGroup
}

func (r *TxQueryGroup) Build(conf *Config, graph Graph) (*TxQueryGroupBuild, error) {
	txBuild := new(TxQueryGroupBuild)
	if r.TransactionIsolationLevel != nil {
		txBuild.IsolationLevel = *r.TransactionIsolationLevel
	} else {
		txBuild.IsolationLevel = conf.Transaction.IsolationLevel
	}

	build, err := r.QueryGroup.Build(conf, graph)
	if err != nil {
		return nil, err
	}

	txBuild.QueryGroupBuild = *build

	return txBuild, nil
}

func (tr *TxQueryGroup) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if len(tr.Searches)+len(tr.Aggregates) <= 1 {
		return 0, 0, &ValidationError{
			Rule: "TransactionUnnecessary",
			Err:  errors.New("transaction with a single search or one aggregate is unnecessary"),
		}
	}
	if tr.TransactionIsolationLevel != nil && !c.Transaction.AllowClientIsolationLevel {
		return 0, 0, &ValidationError{
			Rule: "TransactionClientIsolationLevelDisallow",
			Err:  errors.New("transaction_isolation_level parameter is not allowed"),
		}
	}
	return tr.QueryGroup.ValidateAndPreprocess(c)
}

type QueryGroupBuild struct {
	Searches   []*NamedQueryBuild
	Aggregates []*ScalarQuery
}

type QueryGroup struct {
	Searches   []NamedQuery       `json:"searches,omitempty"`
	Aggregates []OverallAggregate `json:"aggregates,omitempty"`
}

/* func (r *QueryGroup) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	countSearches, countAggregates, err := r.ValidateAndPreprocess(cfg)
	if err != nil {
		return nil, err
	}

	build, err := r.Build(cfg, graph)
	if err != nil {
		return nil, err
	}

	var searchAlone, searchTxPaginate, searchPaginate []*NamedQueryBuild
	for _, s := range build.Searches {
		if s.Paginate != nil {
			if s.EnableTransaction {
				searchTxPaginate = append(searchTxPaginate, s)
			}
			searchPaginate = append(searchPaginate, s)
		}
		searchAlone = append(searchAlone, s)
	}

	if len(searchPaginate) > 0 {
		for _, s := range searchPaginate {
			build.Aggregates = append(build.Aggregates, s.Paginate.ToScalarQuery(s.Key))
		}
	}


	go {
		searchAlone
		searchTx+paginate
		search => feelpaginate
		aggregates => feelpaginate

	}


} */

func (r *QueryGroup) Build(conf *Config, graph Graph) (
	build *QueryGroupBuild,
	err error,
) {
	build = new(QueryGroupBuild)
	if lenght := len(r.Searches); lenght > 0 {
		searches := make([]*NamedQueryBuild, 0, lenght)
		for i, s := range r.Searches {
			searches[i], err = s.Build(i, conf, graph)
			if err != nil {
				return
			}
		}
		build.Searches = searches
	}
	if lenght := len(r.Aggregates); lenght > 0 {
		aggregates := make([]*ScalarQuery, 0, lenght)
		for i, a := range r.Aggregates {
			aggregates[i], err = a.BuildScalar(graph)
			if err != nil {
				return
			}
		}
		build.Aggregates = aggregates
	}
	return
}

func (sr *QueryGroup) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if c.MaxAggregatesPerRequest != 0 && len(sr.Aggregates) > c.MaxAggregatesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxAggregatesPerRequest",
			Err:  fmt.Errorf("found %d aggregates, but the maximum allowed is %d", len(sr.Aggregates), c.MaxAggregatesPerRequest),
		}
	}
	if c.MaxSearchesPerRequest != 0 && len(sr.Searches) > c.MaxSearchesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxSearchesPerRequest",
			Err:  fmt.Errorf("found %d searches, but the maximum allowed is %d", len(sr.Searches), c.MaxSearchesPerRequest),
		}
	}

	for i := range sr.Aggregates {
		if err = sr.Aggregates[i].ValidateAndPreprocess(&c.FilterConfig); err != nil {
			return 0, 0, err
		}
	}
	for i := range sr.Searches {
		if err = sr.Searches[i].ValidateAndPreprocess(c); err != nil {
			return 0, 0, err
		}
	}

	return len(sr.Aggregates), len(sr.Searches), nil
}
