package search

import (
	"context"
	"fmt"

	"entgo.io/ent/dialect/sql"
)

func (sp *From) Prepare(
	conf *Config,
	registry map[string]Node,
	client Client,
) (execute func(ctx context.Context) (any, int, error), countSel *sql.Selector, err error) {
	node, found := registry[sp.From]
	if !found {
		return nil, nil, &ValidationError{
			Rule: "UnknowRootNode",
			Err:  fmt.Errorf("node named %s not found", sp.From),
		}
	}

	return sp.Params.Prepare(conf, node, client)
}

func (sp *Params) Prepare(
	conf *Config,
	node Node,
	client Client,
) (execute func(ctx context.Context) (any, int, error), countSel *sql.Selector, err error) {
	q := node.NewQuery(client)

	if err = sp.Select.Apply(q, node); err != nil {
		return
	}

	if err = sp.Includes.Apply(q, node); err != nil {
		return
	}

	var (
		aggFields []string
		preds     []func(*sql.Selector)
	)
	if ps, fields, err := sp.Aggregates.Predicate(node); err != nil {
		return nil, nil, err
	} else if len(ps) > 0 {
		aggFields = fields
		preds = append(preds, ps...)
	}

	filtPreds, err := sp.Filters.Predicate(node)
	if err != nil {
		return nil, nil, err
	} else if len(filtPreds) > 0 {
		preds = append(preds, filtPreds...)
	}

	if ps, err := sp.Sort.Predicate(node); err != nil {
		return nil, nil, err
	} else if len(ps) > 0 {
		preds = append(preds, ps...)
	}

	preds = append(preds, sp.Pageable.Predicate(true))

	q.Predicate(preds...)

	countSel, err = sp.scalarCountSelector(node, filtPreds...)
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

func (sp *Params) scalarCountSelector(node Node, preds ...func(*sql.Selector)) (*sql.Selector, error) {
	if !sp.WithPagination {
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

func (sp *Params) ValidateAndPreprocess(c *Config) error {
	var err error
	if err = sp.Filters.ValidateAndPreprocess(&c.FilterConfig); err != nil {
		return err
	}
	if err = sp.Includes.ValidateAndPreprocess(&c.IncludeConfig); err != nil {
		return err
	}
	if err = sp.Aggregates.ValidateAndPreprocess(&c.AggregateConfig); err != nil {
		return err
	}
	if err = sp.Sort.ValidateAndPreprocess(&c.SortConfig); err != nil {
		return err
	}
	sp.Pageable.Sanitize(&c.PageableConfig)
	return nil
}

func (sr *HubRequest) ValidateAndPreprocess(c *Config) error {
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
