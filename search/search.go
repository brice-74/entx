package search

import (
	"context"
	"fmt"

	stdsql "database/sql"

	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
	"golang.org/x/sync/errgroup"
)

type NamedQueryBuild struct {
	Key string
	QueryOptionsBuild
}

func (build *NamedQueryBuild) ToTxQueryGroupBuild() *TxQueryGroupBuild {
	return &TxQueryGroupBuild{
		IsolationLevel: build.TransactionIsolationLevel,
		QueryGroupBuild: QueryGroupBuild{
			Searches: []*NamedQueryBuild{build},
		},
	}
}

type NamedQuery struct {
	Key string `json:"key"`
	TargetedQuery
}

func (q *NamedQuery) Build(
	ctx context.Context,
	uniqueIndex int,
	cfg *Config,
	graph entx.Graph,
) (
	*NamedQueryBuild,
	error,
) {
	if q.Key == "" {
		q.Key = fmt.Sprintf("search_%d", uniqueIndex+1)
	}

	build, err := q.TargetedQuery.Build(ctx, cfg, graph)
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
	client entx.Client,
	graph entx.Graph,
	cfg *Config,
) (*SearchResponse, error) {
	ctx, cancel := common.ContextTimeout(common.ContextWithPolicyToken(ctx), cfg.RequestTimeout)
	defer cancel()

	if err := q.ValidateAndPreprocess(cfg); err != nil {
		return nil, err
	}

	build, err := q.Build(ctx, cfg, graph)
	if err != nil {
		return nil, err
	}

	return q.QueryOptions.execute(ctx, client, cfg, build)
}

func (q *TargetedQuery) Build(
	ctx context.Context,
	conf *Config,
	registry entx.Graph,
) (*QueryOptionsBuild, error) {
	node, found := registry[q.From]
	if !found {
		return nil, &ValidationError{
			Rule: "UnknowRootNode",
			Err:  fmt.Errorf("node named %s not found", q.From),
		}
	}

	return q.QueryOptions.Build(ctx, conf, node)
}

type QueryOptions struct {
	Select         dsl.Select     `json:"select,omitempty"`
	Filters        dsl.Filters    `json:"filters,omitempty"`
	Includes       dsl.Includes   `json:"includes,omitempty"`
	Sort           dsl.Sorts      `json:"sort,omitempty"`
	Aggregates     dsl.Aggregates `json:"aggregates,omitempty"`
	WithPagination bool           `json:"with_pagination,omitempty"`
	// Enable transaction between query and pagination.
	// Has no effect if there is no pagination or in a TxQueryGroup.
	EnableTransaction         *bool                  `json:"enable_transaction,omitempty"`
	TransactionIsolationLevel *stdsql.IsolationLevel `json:"transaction_isolation_level,omitempty"`
	dsl.Pageable
}

func (qo *QueryOptions) Execute(
	ctx context.Context,
	client entx.Client,
	node entx.Node,
	cfg *Config,
) (*SearchResponse, error) {
	ctx, cancel := common.ContextTimeout(common.ContextWithPolicyToken(ctx), cfg.RequestTimeout)
	defer cancel()

	if err := qo.ValidateAndPreprocess(cfg); err != nil {
		return nil, err
	}

	build, err := qo.Build(ctx, cfg, node)
	if err != nil {
		return nil, err
	}

	return qo.execute(ctx, client, cfg, build)
}

func (qo *QueryOptions) execute(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
	build *QueryOptionsBuild,
) (*SearchResponse, error) {
	switch {
	case build.IsPaginatedWithTx():
		return build.ExecutePaginatedWithTx(ctx, client, cfg)
	case build.IsPaginatedWithoutTx():
		return build.ExecutePaginatedWithoutTx(ctx, client, cfg)
	default:
		return build.ExecuteSearchOnly(ctx, client, cfg)
	}
}

type QueryOptionsBuild struct {
	ExecFn                    func(context.Context, entx.Client) (any, int, error)
	Paginate                  *common.PaginateInfos
	EnableTransaction         bool
	TransactionIsolationLevel stdsql.IsolationLevel
}

// run asynchronously search query & count paginate inside 2 goroutines
func (build *QueryOptionsBuild) ExecutePaginatedWithoutTx(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
) (*SearchResponse, error) {
	if !build.IsPaginated() {
		panic("cannot call QueryOptionsBuild.ExecutePaginatedWithTx with nil pagination")
	}

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(2)

	var (
		response      *SearchResponse
		paginateCount int
	)

	wg.Go(func() (err error) {
		response, err = build.ExecuteSearchOnly(wgctx, client, cfg)
		return
	})

	wg.Go(func() (err error) {
		paginateCount, err = build.ExecutePaginate(wgctx, client, cfg)
		return
	})

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	response.Meta.Paginate = build.Paginate.Calculate(paginateCount, response.Meta.Count)
	return response, nil
}

// run synchronously search query & count paginate in the same transaction
func (build *QueryOptionsBuild) ExecutePaginatedWithTx(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
) (*SearchResponse, error) {
	if !build.IsPaginatedWithTx() {
		panic("cannot call QueryOptionsBuild.ExecutePaginatedWithTx with nil pagination or without transaction")
	}

	return WithTx(ctx, client, &stdsql.TxOptions{
		ReadOnly:  true,
		Isolation: build.TransactionIsolationLevel,
	}, func(ctx context.Context, client entx.Client) (*SearchResponse, error) {
		response, err := build.ExecuteSearchOnly(ctx, client, cfg)
		if err != nil {
			return nil, err
		}

		total, err := build.ExecutePaginate(ctx, client, cfg)
		if err != nil {
			return nil, err
		}

		response.Meta.Paginate = build.Paginate.Calculate(total, response.Meta.Count)
		return response, nil
	})
}

// execute search without pagination
func (build *QueryOptionsBuild) ExecuteSearchOnly(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
) (*SearchResponse, error) {
	data, count, err := build.ExecFn(ctx, client)
	if err != nil {
		return nil, err
	}

	return &SearchResponse{Data: data, Meta: &MetaSearchResponse{Count: count}}, nil
}

func (build *QueryOptionsBuild) ExecutePaginate(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
) (int, error) {
	if !build.IsPaginated() {
		panic("cannot call QueryOptionsBuild.ExecutePaginate with nil pagination")
	}

	raw, err := common.ExecuteScalar(ctx, client, build.Paginate.ToScalarQuery(""))
	if err != nil {
		return 0, err
	}

	total, ok := raw.(int64)
	if !ok {
		return 0, &ExecError{
			Op:  "QueryOptionsBuild.ExecutePaginate",
			Err: fmt.Errorf("paginate count wrong type: %T", raw),
		}
	}

	return int(total), nil
}

func (build *QueryOptionsBuild) IsSearchOnly() bool {
	return !build.IsPaginatedWithTx() || !build.IsPaginatedWithoutTx()
}
func (build *QueryOptionsBuild) IsPaginated() bool {
	return build.Paginate != nil
}
func (build *QueryOptionsBuild) IsTx() bool {
	return build.EnableTransaction
}
func (build *QueryOptionsBuild) IsPaginatedWithTx() bool {
	return build.Paginate != nil && build.EnableTransaction
}
func (build *QueryOptionsBuild) IsPaginatedWithoutTx() bool {
	return build.Paginate != nil && !build.EnableTransaction
}

func (qo *QueryOptions) Build(
	ctx context.Context,
	cfg *Config,
	node entx.Node,
) (*QueryOptionsBuild, error) {
	var (
		aggFields []string
		preds     []func(*sql.Selector)
	)

	policyPred, err := common.EnforcePolicy(ctx, node, OpRootQuery)
	if err != nil {
		return nil, err
	}

	if policyPred != nil {
		preds = append(preds, policyPred)
	}

	filtPreds, err := qo.Filters.Predicate(node)
	if err != nil {
		return nil, err
	} else if len(filtPreds) > 0 {
		preds = append(preds, filtPreds...)
	}

	// catch preds here to have only filters and policy predictions
	countSel, err := qo.scalarCountSelector(node, cfg.Dialect, preds...)
	if err != nil {
		return nil, err
	}

	if ps, fields, err := qo.Aggregates.Predicate(ctx, node, cfg.Dialect); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		aggFields = fields
		preds = append(preds, ps...)
	}

	if ps, err := qo.Sort.Predicate(node); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		preds = append(preds, ps...)
	}

	preds = append(preds, qo.Pageable.Predicate(true))

	selectApply, err := qo.Select.PredicateQ(node)
	if err != nil {
		return nil, err
	}

	incApplies, err := qo.Includes.PredicateQs(ctx, node, cfg.Dialect)
	if err != nil {
		return nil, err
	}

	execute := func(ctx context.Context, client entx.Client) (any, int, error) {
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
			if err := entx.AddAggregatesFromValues(aggFields...)(entities); err != nil {
				panic(err)
			}
		}
		return entities, len(entities), nil
	}

	res := QueryOptionsBuild{
		ExecFn:                    execute,
		EnableTransaction:         cfg.Transaction.EnablePaginateQuery,
		TransactionIsolationLevel: cfg.Transaction.IsolationLevel,
	}

	if qo.EnableTransaction != nil {
		res.EnableTransaction = *qo.EnableTransaction
	}

	if qo.TransactionIsolationLevel != nil {
		res.TransactionIsolationLevel = *qo.TransactionIsolationLevel
	}

	if countSel != nil {
		res.Paginate = &common.PaginateInfos{
			CountSelector: countSel,
			Page:          qo.Page,
			Limit:         qo.Limit.Limit,
		}
	}

	return &res, err
}

func (qo *QueryOptions) scalarCountSelector(node entx.Node, dialect string, preds ...func(*sql.Selector)) (*sql.Selector, error) {
	if !qo.WithPagination {
		return nil, nil
	}

	sel := sql.Dialect(dialect).Select(sql.Count("*")).
		From(sql.Table(node.Table()).As("t0"))

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
