package search

import (
	"fmt"

	"entgo.io/ent/dialect/sql"
)

func (incs Includes) Apply(q Query, node Node) error {
	for _, inc := range incs {
		if _, err := inc.Apply(q, node); err != nil {
			return err
		}
	}
	return nil
}

func (inc *Include) Apply(q Query, node Node) (Query, error) {
	if !inc.preprocessed {
		panic("Include.Apply: called before preprocess")
	}
	current := node
	var childQ Query
	var bridges = make([]Bridge, 0, len(inc.relationParts))
	for _, rel := range inc.relationParts {
		bridge := current.Bridge(rel)
		if bridge == nil {
			return nil, &QueryBuildError{
				Op:  "Include.Apply",
				Err: fmt.Errorf("relation %q not found on node %q", rel, current.Name()),
			}
		}

		bridges = append(bridges, bridge)
		current = bridge.Child()
	}

	var (
		aggFields []string
		preds     []func(*sql.Selector)
	)
	if ps, fields, err := inc.Aggregates.Predicate(current); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		aggFields = fields
		preds = append(preds, ps...)
	}

	var hasAggregates = len(aggFields) > 0
	for i, bridge := range bridges {
		isLastIndex := len(bridges)-1 == i
		childQ = nil

		if isLastIndex && hasAggregates {
			bridge.Include(q, func(qChild Query) {
				childQ = qChild
			}, AddAggregatesFromValues(aggFields...))
		} else {
			bridge.Include(q, func(qChild Query) { childQ = qChild })
		}

		childQ.Predicate(inc.Pageable.Predicate(isLastIndex))
		q = childQ
	}

	if err := inc.Select.Apply(q, current); err != nil {
		return nil, err
	}

	if ps, err := inc.Filters.Predicate(current); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		preds = append(preds, ps...)
	}

	if ps, err := inc.Sort.Predicate(current); err != nil {
		return nil, err
	} else if len(ps) > 0 {
		preds = append(preds, ps...)
	}

	if len(preds) > 0 {
		q.Predicate(preds...)
	}

	if err := inc.Includes.Apply(q, current); err != nil {
		return nil, err
	}

	return q, nil
}

func (inc *Include) ValidateAndPreprocess(cfg *IncludeConfig) error {
	return Includes{*inc}.ValidateAndPreprocess(cfg)
}

func (incs Includes) ValidateAndPreprocess(cfg *IncludeConfig) error {
	if cfg == nil {
		cfg = &IncludeConfig{}
	}
	total := 0
	for i := range incs {
		if err := incs[i].walkValidate(cfg, 0, &total); err != nil {
			return err
		}
	}
	if cfg.MaxIncludeTreeCount > 0 && total > cfg.MaxIncludeTreeCount {
		return &ValidationError{
			Rule: "MaxIncludeTreeCount",
			Err:  fmt.Errorf("includes count exceeds max %d", cfg.MaxIncludeTreeCount),
		}
	}
	return nil
}

func (inc *Include) walkValidate(cfg *IncludeConfig, depth int, total *int) error {
	parts, pos, ok := splitChain(inc.Relation)
	if !ok {
		return &ValidationError{
			Rule: "InvalidIncludeRelationFormat",
			Err:  fmt.Errorf("invalid empty relation segment at character %d: %s", pos, inc.Relation),
		}
	}
	inc.relationParts = parts

	*total += len(parts)
	depth += len(parts)
	if cfg.MaxIncludeRelationDepth > 0 && depth > cfg.MaxIncludeRelationDepth {
		return &ValidationError{
			Rule: "MaxIncludeRelationsDepth",
			Err:  fmt.Errorf("includes depth exceeds max %d", cfg.MaxIncludeRelationDepth),
		}
	}

	for i := range inc.Includes {
		if err := inc.Includes[i].walkValidate(cfg, depth, total); err != nil {
			return err
		}
	}

	if cfg.FilterConfig != nil {
		if err := inc.Filters.ValidateAndPreprocess(cfg.FilterConfig); err != nil {
			return err
		}
	}

	if cfg.AggregateConfig != nil {
		if err := inc.Aggregates.ValidateAndPreprocess(cfg.AggregateConfig); err != nil {
			return err
		}
	}

	inc.Pageable.Sanitize(cfg.PageableConfig)

	inc.preprocessed = true
	return nil
}
