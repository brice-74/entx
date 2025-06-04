package search

import (
	"errors"
	"fmt"
	"strings"

	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
)

func (ss Sorts) Predicate(node Node) ([]func(*sql.Selector), error) {
	lenSorts := len(ss)
	if lenSorts == 0 {
		return nil, nil
	}

	preds := make([]func(*sql.Selector), 0, lenSorts)
	for _, f := range ss {
		pred, err := f.Predicate(node)
		if err != nil {
			return nil, err
		}
		preds = append(preds, pred)
	}
	return preds, nil
}

func (s *Sort) dirBuilder() (func(string) string, error) {
	switch s.Direction {
	case DirDESC:
		return sql.Desc, nil
	case DirASC, "":
		return sql.Asc, nil
	default:
		return nil, &QueryBuildError{
			Op:  "Sort.dirBuilder",
			Err: fmt.Errorf("unsupported direction %q", s.Direction),
		}
	}
}

func (s *Sort) aggBuilder() (func(string) string, error) {
	if s.Aggregate == "" {
		return func(col string) string { return col }, nil
	}
	switch s.Aggregate {
	case AggAvg:
		return sql.Avg, nil
	case AggSum:
		return sql.Sum, nil
	case AggMin:
		return sql.Min, nil
	case AggMax:
		return sql.Max, nil
	case AggCount:
		return sql.Count, nil
	default:
		return nil, &QueryBuildError{
			Op:  "Sort.dirBuilder",
			Err: fmt.Errorf("unsupported aggregate %q", s.Aggregate),
		}
	}
}

func (s *Sort) Predicate(node Node) (func(*sql.Selector), error) {
	direction, err := s.dirBuilder()
	if err != nil {
		return nil, err
	}
	agg, err := s.aggBuilder()
	if err != nil {
		return nil, err
	}

	_, field, bridges, err := resolveChain(node, s.fieldParts)
	if err != nil {
		return nil, err
	}

	if field == "" {
		if s.Aggregate == AggCount {
			field = "*"
		} else {
			return nil, &QueryBuildError{
				Op:  "Sort.Predicate",
				Err: errors.New("field must be specified"),
			}
		}
	}

	hasAgg := s.Aggregate != ""
	// cannot aggregate without relation
	if hasAgg && len(bridges) == 0 {
		return nil, &QueryBuildError{
			Op:  "Sort.Predicate",
			Err: fmt.Errorf("aggregate %q without relations", s.Aggregate),
		}
	}

	// ensure non-M2O for direct sort
	// for non-aggregate sorts, only M2O relations allowed
	if !hasAgg && len(bridges) > 0 {
		for _, b := range bridges {
			if b.RelInfos().RelType != sqlgraph.M2O {
				return nil, &QueryBuildError{
					Op:  "Sort.Predicate",
					Err: fmt.Errorf("non-aggregate sort through %s not allowed", b.RelInfos().RelType),
				}
			}
		}
	}

	return func(sel *sql.Selector) {
		if len(bridges) == 0 {
			sel.OrderBy(direction(sel.C(field)))
			return
		}

		last := bridges[len(bridges)-1]
		subAlias := last.Child().Table()
		fromTbl := sql.Table(subAlias).As("t0")
		sub := sql.Dialect(sel.Dialect()).Select().From(fromTbl)

		prev := fromTbl
		for i := len(bridges) - 1; i >= 1; i-- {
			rev := bridges[i].Inverse()
			joinTbl := rev.Join(sub, prev)[0]
			prev = joinTbl
		}

		rel := bridges[0].RelInfos()
		keyCol := prev.C(rel.FinalRightField)
		aggCol := "*"
		alias := ""
		if field != "*" {
			aggCol = fromTbl.C(field)
			alias = strings.ToLower(string(s.Aggregate) + "_" + subAlias + "_" + field)
		} else if hasAgg {
			alias = strings.ToLower(string(s.Aggregate) + "_" + subAlias)
		}

		selExpr := agg(aggCol)
		if alias != "" {
			selExpr = sql.As(selExpr, alias)
		}
		sub.Select(keyCol, selExpr).GroupBy(keyCol)

		sel.LeftJoin(sub).
			On(sel.C(rel.FinalLeftField), sub.C(rel.FinalRightField))

		if alias != "" {
			sel.OrderBy(direction(sub.C(alias)))
		} else {
			sel.OrderBy(direction(sub.C(field)))
		}
	}, nil
}

func (ss Sorts) ValidateAndPreprocess(cfg *SortConfig) error {
	for i := range ss {
		if err := ss[i].ValidateAndPreprocess(cfg); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sort) ValidateAndPreprocess(cfg *SortConfig) error {
	switch s.Direction {
	case DirASC, DirDESC, "":
	default:
		return &ValidationError{
			Rule: "SortDirection",
			Err:  fmt.Errorf("unsupported direction %q", s.Direction),
		}
	}

	switch s.Aggregate {
	case AggAvg, AggSum, AggMin, AggMax, AggCount, "":
	default:
		return &ValidationError{
			Rule: "SortAggregate",
			Err:  fmt.Errorf("unsupported aggregate %q", s.Aggregate),
		}
	}

	if s.Field != "" {
		parts, pos, ok := splitChain(s.Field)
		if !ok {
			return &ValidationError{
				Rule: "InvalidSortFieldFormat",
				Err:  fmt.Errorf("invalid empty field segment at character %d: %s", pos, s.Field),
			}
		}

		if cfg.MaxSortRelationDepth > 0 && len(parts)-1 > cfg.MaxSortRelationDepth {
			return &ValidationError{
				Rule: "MaxSortRelationsDepth",
				Err:  fmt.Errorf("aggregate relation depth of %d exceeds max %d", len(parts)-1, cfg.MaxSortRelationDepth),
			}
		}
		s.fieldParts = parts
	}

	s.preprocessed = true
	return nil
}
