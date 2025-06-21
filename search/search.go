package search

import (
	"context"
	"fmt"

	stdsql "database/sql"

	"entgo.io/ent/dialect/sql"
	"golang.org/x/sync/errgroup"
)

type NamedQueries []*NamedQuery

type SearchesResponse = map[string]*SearchResponse

func (queries NamedQueries) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (SearchesResponse, error) {
	// TODO
	return nil, nil
}

func (queries NamedQueries) BuildClassified(
	conf *Config,
	graph Graph,
) (
	searchOnly []*NamedQueryBuild,
	paginatedWithTx []*NamedQueryBuild,
	paginatedWithoutTx []*NamedQueryBuild,
	err error,
) {
	for i, q := range queries {
		build, err := q.Build(i, conf, graph)
		if err != nil {
			return nil, nil, nil, err
		}
		switch true {

		case build.IsPaginatedWithTx():
			paginatedWithTx = append(paginatedWithTx, build)
		case build.IsPaginatedWithoutTx():
			paginatedWithoutTx = append(paginatedWithoutTx, build)
		default:
			searchOnly = append(searchOnly, build)
		}
	}
	return
}

func (queries NamedQueries) Build(conf *Config, graph Graph) ([]*NamedQueryBuild, error) {
	var builds = make([]*NamedQueryBuild, 0, len(queries))
	for i, q := range queries {
		build, err := q.Build(i, conf, graph)
		if err != nil {
			return nil, err
		}
		builds[i] = build
	}
	return builds, nil
}

func (queries NamedQueries) ValidateAndPreprocessFinal(cfg *Config) error {
	count, err := queries.ValidateAndPreprocess(cfg)
	if err != nil {
		return err
	}

	return checkMaxSearches(cfg, count)
}

func (queries NamedQueries) ValidateAndPreprocess(cfg *Config) (count int, err error) {
	for _, q := range queries {
		if err = q.ValidateAndPreprocess(cfg); err != nil {
			return
		}
		count++
	}
	return
}

type NamedQueryBuild struct {
	Key string
	QueryOptionsBuild
}

type NamedQuery struct {
	Key string `json:"key"`
	TargetedQuery
}

func (q *NamedQuery) Build(uniqueIndex int, conf *Config, graph Graph) (
	*NamedQueryBuild,
	error,
) {
	if q.Key == "" {
		q.Key = fmt.Sprintf("search_%d", uniqueIndex+1)
	}

	build, err := q.TargetedQuery.Build(conf, graph)
	if err != nil {
		return nil, err
	}

	return &NamedQueryBuild{
		Key:               q.Key,
		QueryOptionsBuild: *build,
	}, nil
}

type TargetedQuery struct {
	From string `json:"from"`
	QueryOptions
}

func (q *TargetedQuery) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*SearchResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := q.ValidateAndPreprocess(cfg); err != nil {
		return nil, err
	}

	build, err := q.Build(cfg, graph)
	if err != nil {
		return nil, err
	}

	return q.QueryOptions.execute(ctx, client, cfg, build)
}

func (q *TargetedQuery) Build(
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

	return q.QueryOptions.Build(conf, node)
}

type QueryOptions struct {
	Select         Select     `json:"select,omitempty"`
	Filters        Filters    `json:"filters,omitempty"`
	Includes       Includes   `json:"includes,omitempty"`
	Sort           Sorts      `json:"sort,omitempty"`
	Aggregates     Aggregates `json:"aggregates,omitempty"`
	WithPagination bool       `json:"with_pagination,omitempty"`
	// Enable transaction between query and pagination.
	// Has no effect if there is no pagination or in a TxQueryGroup.
	EnableTransaction         *bool                  `json:"enable_transaction,omitempty"`
	TransactionIsolationLevel *stdsql.IsolationLevel `json:"transaction_isolation_level,omitempty"`
	Pageable
}

type SearchMeta struct {
	Paginate *PaginateResponse `json:"paginate,omitempty"`
	Count    int               `json:"count,omitempty"`
}

type SearchResponse struct {
	Data any         `json:"data,omitempty"`
	Meta *SearchMeta `json:"meta,omitempty"`
}

func (qo *QueryOptions) Execute(
	ctx context.Context,
	client Client,
	node Node,
	cfg *Config,
) (*SearchResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := qo.ValidateAndPreprocess(cfg); err != nil {
		return nil, err
	}

	build, err := qo.Build(cfg, node)
	if err != nil {
		return nil, err
	}

	return qo.execute(ctx, client, cfg, build)
}

func (qo *QueryOptions) execute(
	ctx context.Context,
	client Client,
	cfg *Config,
	build *QueryOptionsBuild,
) (*SearchResponse, error) {
	if build.Paginate != nil {
		if build.EnableTransaction {
			// run synchronously search query & count paginate in the same transaction
			return qo.runTx(ctx, client, build, cfg)
		}
		// run asynchronously search query & count paginate inside 2 goroutines
		return qo.runGo(ctx, client, build, cfg)
	}
	// run search without pagination
	ctx, cancel := contextTimeout(ctx, cfg.QueryTimeout)
	defer cancel()

	data, count, err := build.ExecFn(ctx, client)
	if err != nil {
		return nil, err
	}

	return &SearchResponse{Data: data, Meta: &SearchMeta{Count: count}}, nil
}

func (qo *QueryOptions) runGo(
	ctx context.Context,
	client Client,
	build *QueryOptionsBuild,
	cfg *Config,
) (*SearchResponse, error) {
	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(2)

	var (
		searchResponse *SearchResponse
		paginateCount  int
	)

	wg.Go(func() (err error) {
		ctx, cancel := contextTimeout(wgctx, cfg.QueryTimeout)
		defer cancel()

		data, count, err := build.ExecFn(ctx, client)
		if err != nil {
			return err
		}

		searchResponse = &SearchResponse{Data: data, Meta: &SearchMeta{Count: count}}
		return nil
	})

	wg.Go(func() (err error) {
		ctx, cancel := contextTimeout(wgctx, cfg.AggregateTimeout)
		defer cancel()

		raw, err := ExecuteScalar(ctx, client, build.Paginate.ToScalarQuery(""))
		if err != nil {
			return err
		}

		total, ok := raw.(int64)
		if !ok {
			return &ExecError{
				Op:  "QueryOptions.runGo",
				Err: fmt.Errorf("paginate count wrong type: %T", raw),
			}
		}

		paginateCount = int(total)
		return nil
	})

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	searchResponse.Meta.Paginate = build.Paginate.Calculate(int(paginateCount), searchResponse.Meta.Count)

	return searchResponse, nil
}

func (qo *QueryOptions) runTx(
	ctx context.Context,
	client Client,
	build *QueryOptionsBuild,
	cfg *Config,
) (*SearchResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.Transaction.Timeout)
	defer cancel()

	return WithTx(ctx, client, &stdsql.TxOptions{
		ReadOnly:  true,
		Isolation: build.TransactionIsolationLevel,
	}, func(ctx context.Context, client Client) (*SearchResponse, error) {
		data, count, err := build.ExecFn(ctx, client)
		if err != nil {
			return nil, err
		}

		raw, err := ExecuteScalar(ctx, client, build.Paginate.ToScalarQuery(""))
		if err != nil {
			return nil, err
		}

		total, ok := raw.(int64)
		if !ok {
			return nil, &ExecError{
				Op:  "QueryOptions.runGo",
				Err: fmt.Errorf("paginate count wrong type: %T", raw),
			}
		}

		return &SearchResponse{
			Data: data,
			Meta: &SearchMeta{
				Count:    count,
				Paginate: build.Paginate.Calculate(int(total), count),
			},
		}, nil
	})
}

type QueryOptionsBuild struct {
	ExecFn                    func(context.Context, Client) (any, int, error)
	Paginate                  *PaginateInfos
	EnableTransaction         bool
	TransactionIsolationLevel stdsql.IsolationLevel
}

func (build *QueryOptionsBuild) IsSearchOnly() bool {
	return !build.IsPaginatedWithTx() || !build.IsPaginatedWithoutTx()
}
func (build *QueryOptionsBuild) IsPaginated() bool {
	return build.Paginate != nil
}
func (build *QueryOptionsBuild) IsPaginatedWithTx() bool {
	return build.Paginate != nil && build.EnableTransaction
}
func (build *QueryOptionsBuild) IsPaginatedWithoutTx() bool {
	return build.Paginate != nil && !build.EnableTransaction
}

func (qo *QueryOptions) Build(
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
		q := node.NewQuery(client).Predicate(preds...)

		for _, apply := range incApplies {
			apply(q)
		}

		selectApply(q)

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
		ExecFn:                    execute,
		EnableTransaction:         conf.Transaction.EnablePaginateQuery,
		TransactionIsolationLevel: conf.Transaction.IsolationLevel,
	}

	if qo.EnableTransaction != nil {
		res.EnableTransaction = *qo.EnableTransaction
	}

	if qo.TransactionIsolationLevel != nil {
		res.TransactionIsolationLevel = *qo.TransactionIsolationLevel
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

type Select []string

func (s Select) PredicateQ(node Node) (func(q Query), error) {
	if len(s) > 0 {
		for i, v := range s {
			f := node.FieldByName(v)
			if f == nil {
				return nil, &QueryBuildError{
					Op:  "Select.PredicateQ",
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
