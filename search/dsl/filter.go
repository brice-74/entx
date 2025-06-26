package dsl

import (
	"fmt"

	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
)

type Operator string

const (
	OpEmpty        Operator = ""
	OpEqual        Operator = "="
	OpNotEqual     Operator = "!="
	OpGreaterThan  Operator = ">"
	OpGreaterEqual Operator = ">="
	OpLessThan     Operator = "<"
	OpLessEqual    Operator = "<="
	OpLike         Operator = "LIKE"
	OpNotLike      Operator = "NOT LIKE"
	OpIn           Operator = "IN"
	OpNotIn        Operator = "NOT IN"
)

type Filters []*Filter

func (fs Filters) Predicate(node entx.Node) ([]func(*sql.Selector), error) {
	if len(fs) == 0 {
		return nil, nil
	}

	preds := make([]func(*sql.Selector), len(fs))
	for i, f := range fs {
		pred, err := f.Predicate(node)
		if err != nil {
			return nil, err
		}
		preds[i] = pred
	}
	return preds, nil
}

func (fs Filters) ValidateAndPreprocess(cfg *common.FilterConfig) error {
	if cfg == nil {
		cfg = &common.FilterConfig{}
	}
	var (
		totalFilters   int
		totalRelations int
	)
	for i := range fs {
		if err := fs[i].walkValidate(cfg.MaxRelationChainDepth, 0, &totalFilters, &totalRelations); err != nil {
			return err
		}
	}
	if cfg.MaxFilterTreeCount > 0 && totalFilters > cfg.MaxFilterTreeCount {
		return &common.ValidationError{
			Rule: "MaxFilterTreeCount",
			Err:  fmt.Errorf("filters count exceeds max %d", cfg.MaxFilterTreeCount),
		}
	}
	if cfg.MaxRelationTotalCount > 0 && totalRelations > cfg.MaxRelationTotalCount {
		return &common.ValidationError{
			Rule: "MaxFilterRelationsPerTree",
			Err:  fmt.Errorf("relations count exceeds max %d", cfg.MaxRelationTotalCount),
		}
	}
	return nil
}

type Filter struct {
	Not      *Filter  `json:"not,omitempty"`
	And      Filters  `json:"and,omitempty"`
	Or       Filters  `json:"or,omitempty"`
	Relation string   `json:"relation,omitempty"`
	Field    string   `json:"field,omitempty"`
	Operator Operator `json:"operator,omitempty"`
	Value    any      `json:"value,omitempty"`
	// pre-processed segments
	relationParts []string
	fieldParts    []string
	preprocessed  bool
}

func (f *Filter) Predicate(node entx.Node) (func(*sql.Selector), error) {
	if !f.preprocessed {
		panic("Filter.Predicate: called before preprocess")
	}
	if len(f.relationParts) > 0 {
		finalNode, compose, err := resolveFilterChain(node, f.relationParts)
		if err != nil {
			return nil, &common.QueryBuildError{
				Op:  "Filter.Predicate",
				Err: err,
			}
		}
		local, err := f.localPredicate(finalNode)
		if err != nil {
			return nil, err
		}
		return compose(local), nil
	}
	return f.localPredicate(node)
}

// resolveFilterChain navigates a sequence of relations, returning the final Node
// and a function to wrap a predicate across the chain.
func resolveFilterChain(node entx.Node, rels []string) (entx.Node, func(func(*sql.Selector)) func(*sql.Selector), error) {
	final, _, bridges, err := resolveChain(node, rels)
	if err != nil {
		return nil, nil, err
	}

	compose := func(p func(*sql.Selector)) func(*sql.Selector) { return p }
	for i := len(bridges) - 1; i >= 0; i-- {
		b := bridges[i]
		prev := compose
		compose = func(p func(*sql.Selector)) func(*sql.Selector) {
			return b.FilterWith(prev(p))
		}
	}
	return final, compose, nil
}

// localPredicate builds predicates for Not, Or, And and the leaf condition.
func (f *Filter) localPredicate(node entx.Node) (func(*sql.Selector), error) {
	var preds []func(*sql.Selector)

	if f.Not != nil {
		p, err := f.Not.Predicate(node)
		if err != nil {
			return nil, err
		}
		preds = append(preds, sql.NotPredicates(p))
	}

	if orPreds, err := f.Or.Predicate(node); err != nil {
		return nil, err
	} else if len(orPreds) > 0 {
		preds = append(preds, sql.OrPredicates(orPreds...))
	}

	if andPreds, err := f.And.Predicate(node); err != nil {
		return nil, err
	} else if len(andPreds) > 0 {
		preds = append(preds, sql.AndPredicates(andPreds...))
	}

	if f.Field != "" {
		p, err := f.buildCondition(node)
		if err != nil {
			return nil, err
		}
		preds = append(preds, p)
	}

	switch len(preds) {
	case 1:
		return preds[0], nil
	default:
		return sql.AndPredicates(preds...), nil
	}
}

func (f *Filter) buildCondition(node entx.Node) (func(*sql.Selector), error) {
	lenFieldParts := len(f.fieldParts)
	relations := f.fieldParts[:lenFieldParts-1]
	field := f.fieldParts[lenFieldParts-1]

	if lenFieldParts > 1 {
		_, compose, err := resolveFilterChain(node, relations)
		if err != nil {
			return nil, &common.QueryBuildError{
				Op:  "Filter.buildCondition",
				Err: err,
			}
		}

		base, err := buildBasePredicate(field, f.Operator, f.Value)
		if err != nil {
			return nil, err
		}

		return compose(base), nil
	}

	base, err := buildBasePredicate(field, f.Operator, f.Value)
	if err != nil {
		return nil, &common.QueryBuildError{
			Op:  "Filter.buildCondition",
			Err: err,
		}
	}
	return base, nil
}

func buildBasePredicate(
	field string,
	op Operator,
	value any,
) (func(*sql.Selector), error) {
	switch op {
	case OpEqual:
		return func(s *sql.Selector) { s.Where(sql.EQ(s.C(field), value)) }, nil
	case OpNotEqual:
		return func(s *sql.Selector) { s.Where(sql.NEQ(s.C(field), value)) }, nil
	case OpGreaterThan:
		return func(s *sql.Selector) { s.Where(sql.GT(s.C(field), value)) }, nil
	case OpGreaterEqual:
		return func(s *sql.Selector) { s.Where(sql.GTE(s.C(field), value)) }, nil
	case OpLessThan:
		return func(s *sql.Selector) { s.Where(sql.LT(s.C(field), value)) }, nil
	case OpLessEqual:
		return func(s *sql.Selector) { s.Where(sql.LTE(s.C(field), value)) }, nil
	case OpLike:
		return func(s *sql.Selector) { s.Where(sql.Like(s.C(field), fmt.Sprintf("%%%v%%", value))) }, nil
	case OpNotLike:
		return func(s *sql.Selector) { s.Where(sql.Not(sql.Like(s.C(field), fmt.Sprintf("%%%v%%", value)))) }, nil
	case OpIn:
		return func(s *sql.Selector) { s.Where(sql.In(s.C(field), value.([]any)...)) }, nil
	case OpNotIn:
		return func(s *sql.Selector) { s.Where(sql.Not(sql.In(s.C(field), value.([]any)...))) }, nil
	default:
		return nil, fmt.Errorf("invalid operator %q", op)
	}
}

func (f *Filter) ValidateAndPreprocess(cfg *common.FilterConfig) error {
	return Filters{f}.ValidateAndPreprocess(cfg)
}

func (f *Filter) walkValidate(maxDepth, currentDepth int, totalFilters, totalRelations *int) error {
	*totalFilters++

	if f.Relation != "" {
		parts, pos, ok := splitChain(f.Relation)
		if !ok {
			return &common.ValidationError{
				Rule: "InvalidFilterRelationFormat",
				Err:  fmt.Errorf("invalid empty relation segment at character %d: %s", pos, f.Relation),
			}
		}
		f.relationParts = parts
		currentDepth += len(parts)
		*totalRelations += len(parts)
	}

	if f.Field != "" {
		parts, pos, ok := splitChain(f.Field)
		if !ok {
			return &common.ValidationError{
				Rule: "InvalidFilterFieldFormat",
				Err:  fmt.Errorf("invalid empty field segment at character %d: %s", pos, f.Relation),
			}
		}
		f.fieldParts = parts
		if len(parts) > 1 {
			currentDepth += len(parts) - 1
			*totalRelations += len(parts) - 1
		}
	}

	switch op := f.Operator; op {
	case OpEmpty:
	case OpIn, OpNotIn:
		if !IsSliceOfPrimitiveAnys(f.Value) {
			return &common.ValidationError{
				Rule: "OperatorValue",
				Err:  fmt.Errorf("'%s' operator need slice value of primitive types, got %T", op, f.Value),
			}
		}
	default:
		if !IsPrimitive(f.Value) {
			return &common.ValidationError{
				Rule: "OperatorValue",
				Err:  fmt.Errorf("'%s' operator need primitive type value, got %T", op, f.Value),
			}
		}
	}

	if maxDepth > 0 && currentDepth > maxDepth {
		return &common.ValidationError{
			Rule: "MaxRelationChainDepth",
			Err:  fmt.Errorf("filters nesting depth exceeds max %d", maxDepth),
		}
	}

	if f.Not != nil {
		if err := f.Not.walkValidate(maxDepth, currentDepth, totalFilters, totalRelations); err != nil {
			return err
		}
	}
	for i := range f.And {
		if err := f.And[i].walkValidate(maxDepth, currentDepth, totalFilters, totalRelations); err != nil {
			return err
		}
	}
	for i := range f.Or {
		if err := f.Or[i].walkValidate(maxDepth, currentDepth, totalFilters, totalRelations); err != nil {
			return err
		}
	}

	f.preprocessed = true
	return nil
}

func IsPrimitive(val any) bool {
	switch val.(type) {
	case bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		string:
		return true
	default:
		return false
	}
}

func IsSliceOfPrimitiveAnys(val any) bool {
	if slice, ok := val.([]any); ok {
		for _, v := range slice {
			if !IsPrimitive(v) {
				return false
			}
		}
		return true
	}
	return false
}
