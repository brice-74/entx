package search

import (
	"context"
	"errors"
	"fmt"

	"entgo.io/ent/dialect/sql"
)

type CompositeSearch struct {
	Key      string
	ExecFn   func(context.Context, Client) (any, int, error)
	Paginate *PaginateInfos
}

func (q *NamedQuery) PrepareComposite(uniqueIndex int, conf *Config, graph Graph) (
	*CompositeSearch,
	error,
) {
	if q.Key == "" {
		q.Key = fmt.Sprintf("s%d", uniqueIndex+1)
	}

	exec, countSel, err := q.TargetedQuery.Prepare(conf, graph)
	if err != nil {
		return nil, err
	}

	c := CompositeSearch{
		Key:    q.Key,
		ExecFn: exec,
	}

	if countSel != nil {
		c.Paginate = &PaginateInfos{
			CountSelector: countSel,
			Page:          q.Page,
			Limit:         q.Limit.Limit,
		}
	}

	return &c, nil
}

func (q *TargetedQuery) Prepare(
	conf *Config,
	registry map[string]Node,
) (execute func(ctx context.Context, client Client) (any, int, error), countSel *sql.Selector, err error) {
	node, found := registry[q.From]
	if !found {
		return nil, nil, &ValidationError{
			Rule: "UnknowRootNode",
			Err:  fmt.Errorf("node named %s not found", q.From),
		}
	}

	return q.QueryOptions.Prepare(conf, node)
}

func (qo *QueryOptions) Prepare(
	conf *Config,
	node Node,
) (execute func(ctx context.Context, client Client) (any, int, error), countSel *sql.Selector, err error) {
	var (
		aggFields []string
		preds     []func(*sql.Selector)
	)
	if ps, fields, err := qo.Aggregates.Predicate(node); err != nil {
		return nil, nil, err
	} else if len(ps) > 0 {
		aggFields = fields
		preds = append(preds, ps...)
	}

	filtPreds, err := qo.Filters.Predicate(node)
	if err != nil {
		return nil, nil, err
	} else if len(filtPreds) > 0 {
		preds = append(preds, filtPreds...)
	}

	if ps, err := qo.Sort.Predicate(node); err != nil {
		return nil, nil, err
	} else if len(ps) > 0 {
		preds = append(preds, ps...)
	}

	preds = append(preds, qo.Pageable.Predicate(true))

	countSel, err = qo.scalarCountSelector(node, filtPreds...)
	if err != nil {
		return
	}

	selectApply, err := qo.Select.PredicateApplicator(node)
	if err != nil {
		return
	}

	incApplies, err := qo.Includes.PredicateApplicators(node)
	if err != nil {
		return
	}

	execute = func(ctx context.Context, client Client) (any, int, error) {
		q := node.NewQuery(client)

		for _, apply := range incApplies {
			apply(q)
		}

		selectApply(q)

		q.Predicate(preds...)

		entities, err := q.All(ctx)
		if err != nil {
			return nil, 0, err
		}
		if len(aggFields) > 0 {
			if err := AddAggregatesFromValues(aggFields...)(entities); err != nil {
				return nil, 0, err
			}
		}
		return entities, len(entities), err
	}

	return
}

func (s Select) PredicateApplicator(node Node) (func(q Query), error) {
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

// ------------------------------
// Validations
// ------------------------------

func (qo *QueryOptions) ValidateAndPreprocess(c *Config) error {
	var err error
	if err = qo.Filters.ValidateAndPreprocess(&c.FilterConfig); err != nil {
		return err
	}
	if err = qo.Includes.ValidateAndPreprocess(&c.IncludeConfig); err != nil {
		return err
	}
	if err = qo.Aggregates.ValidateAndPreprocess(&c.AggregateConfig); err != nil {
		return err
	}
	if err = qo.Sort.ValidateAndPreprocess(&c.SortConfig); err != nil {
		return err
	}
	qo.Pageable.Sanitize(&c.PageableConfig)
	return nil
}

func (r *RequestBundle) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if len(r.Transactions) > 0 && !c.Transaction.EnableClientGroupsInput {
		return 0, 0, &ValidationError{
			Rule: "TransactionGroupsInputDisable",
			Err:  errors.New("transactions groups usage is not allowed"),
		}
	}

	totalAgg, totalSearch := 0, 0

	agg, search, err := r.CompositeRequest.ValidateAndPreprocess(c)
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

	for i1 := range r.ParralelGroups {
		for i2 := range r.ParralelGroups[i1] {
			if err = r.ParralelGroups[i1][i2].ValidateAndPreprocess(&c.FilterConfig); err != nil {
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

func (tr *TransactionRequest) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
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
	return tr.CompositeRequest.ValidateAndPreprocess(c)
}

func (sr *CompositeRequest) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
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
