package search

import (
	"context"
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

	selectApply, err := qo.Select.PredicateQ(node)
	if err != nil {
		return
	}

	incApplies, err := qo.Includes.PredicateQs(node)
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
