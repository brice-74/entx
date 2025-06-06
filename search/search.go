package search

import (
	"context"
	"fmt"

	"entgo.io/ent/dialect/sql"
)

type CompositeSearch struct {
	Key      string
	ExecFn   func(context.Context) (any, int, error)
	Paginate *PaginateInfos
}

func (q *NamedQuery) PrepareComposite(uniqueIndex int, conf *Config, graph Graph, client Client) (
	*CompositeSearch,
	error,
) {
	if q.Key == "" {
		q.Key = fmt.Sprintf("s%d", uniqueIndex+1)
	}

	exec, countSel, err := q.TargetedQuery.Prepare(conf, graph, client)
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
			Limit:         q.Limit,
		}
	}

	return &c, nil
}

func (q *TargetedQuery) Prepare(
	conf *Config,
	registry map[string]Node,
	client Client,
) (execute func(ctx context.Context) (any, int, error), countSel *sql.Selector, err error) {
	node, found := registry[q.From]
	if !found {
		return nil, nil, &ValidationError{
			Rule: "UnknowRootNode",
			Err:  fmt.Errorf("node named %s not found", q.From),
		}
	}

	return q.QueryOptions.Prepare(conf, node, client)
}

func (qo *QueryOptions) Prepare(
	conf *Config,
	node Node,
	client Client,
) (execute func(ctx context.Context) (any, int, error), countSel *sql.Selector, err error) {
	q := node.NewQuery(client)

	if err = qo.Select.Apply(q, node); err != nil {
		return
	}

	if err = qo.Includes.Apply(q, node); err != nil {
		return
	}

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

	q.Predicate(preds...)

	countSel, err = qo.scalarCountSelector(node, filtPreds...)
	if err != nil {
		return
	}

	execute = func(ctx context.Context) (any, int, error) {
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

/*
	 func (qo *QueryOptions) Execute(
		ctx context.Context,
		conf *Config,
		node Node,
		client Client,

	) (*SearchResponse, error) {
		exec, countSel, err := qo.Prepare(conf, node, client)
		if err != nil {
			return nil, err
		}
		query, args := countSel.Query()
		rows, err := client.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if !rows.Next() {
			return nil, fmt.Errorf("no rows returned")
		}
		var count *sql.NullInt64
		if err := rows.Scan(count); err != nil {
			return nil, err
		}
	}
*/
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

func (sr *CompositeRequest) ValidateAndPreprocess(c *Config) error {
	if max, got := c.MaxAggregatesPerRequest, len(sr.Aggregates); max != 0 && got > max {
		return &ValidationError{
			Rule: "MaxAggregatesPerRequest",
			Err:  fmt.Errorf("found %d aggregates, but the maximum allowed is %d", got, max),
		}
	}
	if max, got := c.MaxSearchesPerRequest, len(sr.Searches); max != 0 && got > max {
		return &ValidationError{
			Rule: "MaxSearchesPerRequest",
			Err:  fmt.Errorf("found %d searches, but the maximum allowed is %d", got, max),
		}
	}
	var err error
	for i := range sr.Aggregates {
		if err = sr.Aggregates[i].ValidateAndPreprocess(&c.FilterConfig); err != nil {
			return err
		}
	}
	for i := range sr.Searches {
		if err = sr.Searches[i].ValidateAndPreprocess(c); err != nil {
			return err
		}
	}
	return nil
}

func (s Select) Apply(q Query, node Node) error {
	if len(s) > 0 {
		for i, v := range s {
			f := node.FieldByName(v)
			if f == nil {
				return &QueryBuildError{
					Op:  "Include.Apply",
					Err: fmt.Errorf("node %q has no field named %q", node.Name(), v),
				}
			}
			s[i] = f.StorageName
		}

		q.Select(s...)
	}

	return nil
}
