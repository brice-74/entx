package search

import (
	"context"
	"fmt"

	"entgo.io/ent/dialect/sql"
)

type NamedQueryBuild struct {
	Key string
	QueryOptionsBuild
}

type NamedQuery struct {
	Key string `json:"key"`
	TargetedQuery
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

type TargetedQuery struct {
	From string `json:"from"`
	QueryOptions
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

type QueryOptions struct {
	Select         Select     `json:"select,omitempty"`
	Filters        Filters    `json:"filters,omitempty"`
	Includes       Includes   `json:"includes,omitempty"`
	Sort           Sorts      `json:"sort,omitempty"`
	Aggregates     Aggregates `json:"aggregates,omitempty"`
	WithPagination bool       `json:"with_pagination,omitempty"`
	Pageable
}

/*
	 func (qo *QueryOptions) Execute(
		ctx context.Context,
		client Client,
		node Node,
		cfg *Config,

	) (*QueryOptionsBuild, error) {
		ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
		defer cancel()

		if err := qo.ValidateAndPreprocess(cfg); err != nil {
			return nil, err
		}

		build, err := qo.Prepare(cfg, node)
		if err != nil {
			return nil, err
		}

}
*/
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
