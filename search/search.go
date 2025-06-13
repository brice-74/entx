package search

import (
	"context"
	"errors"
	"fmt"

	"entgo.io/ent/dialect/sql"

	stdsql "database/sql"
)

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

func (r *TxQueryGroup) PrepareTxGroup(conf *Config, graph Graph) (
	group *TxGroup,
	err error,
) {
	group = new(TxGroup)
	if r.TransactionIsolationLevel != nil {
		group.IsolationLevel = stdsql.IsolationLevel(*r.TransactionIsolationLevel)
	} else {
		group.IsolationLevel = conf.Transaction.IsolationLevel
	}

	group.Searches, group.Aggregates, err = r.QueryGroup.Prepare(conf, graph)
	return
}

func (r *QueryGroup) Prepare(conf *Config, graph Graph) (
	searches []*NamedQueryBuild,
	aggregates ScalarGroup,
	err error,
) {
	if lenght := len(r.Searches); lenght > 0 {
		searches = make([]*NamedQueryBuild, 0, lenght)
		for i, s := range r.Searches {
			searches[i], err = s.Prepare(i, conf, graph)
			if err != nil {
				return
			}
		}
	}
	if lenght := len(r.Aggregates); lenght > 0 {
		aggregates = make(ScalarGroup, 0, lenght)
		for i, a := range r.Aggregates {
			aggregates[i], err = a.PrepareScalar(graph)
			if err != nil {
				return
			}
		}
	}
	return
}

type NamedQueryBuild struct {
	Key string
	QueryOptionsBuild
}

func (q *NamedQuery) Prepare(uniqueIndex int, conf *Config, graph Graph) (
	*NamedQueryBuild,
	error,
) {
	if q.Key == "" {
		q.Key = fmt.Sprintf("s%d", uniqueIndex+1)
	}

	build, err := q.TargetedQuery.Prepare(conf, graph)
	if err != nil {
		return nil, err
	}

	return &NamedQueryBuild{
		Key:               q.Key,
		QueryOptionsBuild: *build,
	}, nil
}

func (q *TargetedQuery) Prepare(
	conf *Config,
	registry Graph,
) (*QueryOptionsBuild, error) {
	node, found := registry[q.From]
	if !found {
		return nil, &ValidationError{
			Rule: "UnknowRootNode",
			Err:  fmt.Errorf("node named %s not found", q.From),
		}
	}

	return q.QueryOptions.Prepare(conf, node)
}

type QueryOptionsBuild struct {
	ExecFn   func(context.Context, Client) (any, int, error)
	Paginate *PaginateInfos
}

func (qo *QueryOptions) Prepare(
	conf *Config,
	node Node,
) (*QueryOptionsBuild, error) {
	var (
		aggFields []string
		preds     []func(*sql.Selector)
	)
	if ps, fields, err := qo.Aggregates.Predicate(node); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		aggFields = fields
		preds = append(preds, ps...)
	}

	filtPreds, err := qo.Filters.Predicate(node)
	if err != nil {
		return nil, err
	} else if len(filtPreds) > 0 {
		preds = append(preds, filtPreds...)
	}

	if ps, err := qo.Sort.Predicate(node); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		preds = append(preds, ps...)
	}

	preds = append(preds, qo.Pageable.Predicate(true))

	countSel, err := qo.scalarCountSelector(node, filtPreds...)
	if err != nil {
		return nil, err
	}

	selectApply, err := qo.Select.PredicateQ(node)
	if err != nil {
		return nil, err
	}

	incApplies, err := qo.Includes.PredicateQs(node)
	if err != nil {
		return nil, err
	}

	execute := func(ctx context.Context, client Client) (any, int, error) {
		q := node.NewQuery(client)

		for _, apply := range incApplies {
			apply(q)
		}

		selectApply(q)

		q.Predicate(preds...)

		entities, err := q.All(ctx)
		if err != nil {
			return nil, 0, &ExecError{
				Op:  "QueryOptions.execute",
				Err: err,
			}
		}
		if len(aggFields) > 0 {
			if err := AddAggregatesFromValues(aggFields...)(entities); err != nil {
				panic(err)
			}
		}
		return entities, len(entities), nil
	}

	res := QueryOptionsBuild{
		ExecFn: execute,
	}

	if countSel != nil {
		res.Paginate = &PaginateInfos{
			CountSelector: countSel,
			Page:          qo.Page,
			Limit:         qo.Limit.Limit,
		}
	}

	return &res, err
}

func (qo *QueryOptions) scalarCountSelector(node Node, preds ...func(*sql.Selector)) (*sql.Selector, error) {
	if !qo.WithPagination {
		return nil, nil
	}

	sel := sql.Select(sql.Count("*")).
		From(sql.Table(node.Table()).As("t0"))

	if pol := node.Policy(); pol != nil {
		if err := pol.EvalQuery(sel.Context(), nil); err != nil {
			return nil, err
		}
	}

	for _, p := range preds {
		p(sel)
	}

	return sel, nil
}

func (s Select) PredicateQ(node Node) (func(q Query), error) {
	if len(s) > 0 {
		for i, v := range s {
			f := node.FieldByName(v)
			if f == nil {
				return nil, &QueryBuildError{
					Op:  "Include.Apply",
					Err: fmt.Errorf("node %q has no field named %q", node.Name(), v),
				}
			}
			s[i] = f.StorageName
		}

		return func(q Query) {
			q.Select(s...)
		}, nil
	}

	return func(q Query) {}, nil
}

// ------------------------------
// Validations
// ------------------------------

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

func (qo *QueryOptions) ValidateAndPreprocess(c *Config) (err error) {
	if err = qo.Filters.ValidateAndPreprocess(&c.FilterConfig); err != nil {
		return
	}
	if err = qo.Includes.ValidateAndPreprocess(&c.IncludeConfig); err != nil {
		return
	}
	if err = qo.Aggregates.ValidateAndPreprocess(&c.AggregateConfig); err != nil {
		return
	}
	if err = qo.Sort.ValidateAndPreprocess(&c.SortConfig); err != nil {
		return
	}
	qo.Pageable.Sanitize(&c.PageableConfig)
	return
}
