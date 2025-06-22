package dsl

import (
	"fmt"

	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
)

type Includes []*Include

func (incs Includes) PredicateQs(node entx.Node) ([]func(entx.Query), error) {
	var applies = make([]func(entx.Query), 0, len(incs))
	for i, inc := range incs {
		applicator, err := inc.PredicateQ(node)
		if err != nil {
			return nil, err
		}
		applies[i] = applicator
	}
	return applies, nil
}

func (incs Includes) ValidateAndPreprocess(cfg *common.IncludeConfig) error {
	if cfg == nil {
		cfg = &common.IncludeConfig{}
	}
	total := 0
	for i := range incs {
		if err := incs[i].walkValidate(cfg, 0, &total); err != nil {
			return err
		}
	}
	if cfg.MaxIncludeTreeCount > 0 && total > cfg.MaxIncludeTreeCount {
		return &common.ValidationError{
			Rule: "MaxIncludeTreeCount",
			Err:  fmt.Errorf("includes count exceeds max %d", cfg.MaxIncludeTreeCount),
		}
	}
	return nil
}

type Include struct {
	Relation   string     `json:"relation"`
	Select     Select     `json:"select,omitempty"`
	Filters    Filters    `json:"filters,omitempty"`
	Includes   Includes   `json:"includes,omitempty"`
	Sort       Sorts      `json:"sort,omitempty"`
	Aggregates Aggregates `json:"aggregates,omitempty"`
	Limit
	// pre-processed segments
	relationParts []string
	preprocessed  bool
}

func (inc *Include) PredicateQ(node entx.Node) (func(entx.Query), error) {
	if !inc.preprocessed {
		panic("Include.PredicateQ: called before preprocess")
	}
	current := node
	var bridges = make([]entx.Bridge, 0, len(inc.relationParts))
	for _, rel := range inc.relationParts {
		bridge := current.Bridge(rel)
		if bridge == nil {
			return nil, &common.QueryBuildError{
				Op:  "Include.PredicateQ",
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

	selectApply, err := inc.Select.PredicateQ(current)
	if err != nil {
		return nil, err
	}

	incApplies, err := inc.Includes.PredicateQs(current)
	if err != nil {
		return nil, err
	}

	return func(q entx.Query) {
		var (
			childQ        entx.Query
			hasAggregates = len(aggFields) > 0
		)
		for i, bridge := range bridges {
			isLastIndex := len(bridges)-1 == i
			childQ = nil

			if isLastIndex && hasAggregates {
				bridge.Include(q, func(qChild entx.Query) {
					childQ = qChild
				}, entx.AddAggregatesFromValues(aggFields...))
			} else {
				bridge.Include(q, func(qChild entx.Query) { childQ = qChild })
			}

			childQ.Predicate(inc.Limit.Predicate())
			q = childQ
		}

		if len(preds) > 0 {
			q.Predicate(preds...)
		}

		for _, apply := range incApplies {
			apply(q)
		}

		selectApply(q)
	}, nil
}

func (inc *Include) ValidateAndPreprocess(cfg *common.IncludeConfig) error {
	return Includes{inc}.ValidateAndPreprocess(cfg)
}

func (inc *Include) walkValidate(cfg *common.IncludeConfig, depth int, total *int) error {
	parts, pos, ok := splitChain(inc.Relation)
	if !ok {
		return &common.ValidationError{
			Rule: "InvalidIncludeRelationFormat",
			Err:  fmt.Errorf("invalid empty relation segment at character %d: %s", pos, inc.Relation),
		}
	}
	inc.relationParts = parts

	*total += len(parts)
	depth += len(parts)
	if cfg.MaxIncludeRelationDepth > 0 && depth > cfg.MaxIncludeRelationDepth {
		return &common.ValidationError{
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

	inc.Limit.Sanitize(cfg.PageableConfig)

	inc.preprocessed = true
	return nil
}
