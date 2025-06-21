package search

import (
	"context"
	"fmt"
	"strings"

	"entgo.io/ent/dialect/sql"
	"golang.org/x/sync/errgroup"
)

type Agg string

const (
	AggAvg   Agg = "avg"
	AggSum   Agg = "sum"
	AggMin   Agg = "min"
	AggMax   Agg = "max"
	AggCount Agg = "count"
)

type BaseAggregate struct {
	Field    string  `json:"field"`
	Alias    string  `json:"alias,omitempty"`
	Type     Agg     `json:"type"`
	Distinct bool    `json:"distinct,omitempty"`
	Filters  Filters `json:"filters,omitempty"`
	// pre-processed segments
	fieldParts   []string
	preprocessed bool
}

// buildExpr builds the aggregate function, expression, and alias.
func (b *BaseAggregate) buildExpr(tbl *sql.SelectTable, resolvedField string) (
	fn func(string) string, expr string, alias string, err error,
) {
	if !b.preprocessed {
		panic("BaseAggregate.buildExpr: called before preprocess")
	}

	if resolvedField == "" {
		if b.Distinct {
			return nil, "", "", &QueryBuildError{
				Op:  "BaseAggregate.buildExpr",
				Err: fmt.Errorf("cannot use DISTINCT with wildcard '*'; specify a column"),
			}
		}
		if b.Type != AggCount {
			return nil, "", "", &QueryBuildError{
				Op:  "BaseAggregate.buildExpr",
				Err: fmt.Errorf("aggregate %q on '*' is invalid; only COUNT(*) is allowed", b.Type),
			}
		}
		resolvedField = "*"
	}

	switch b.Type {
	case AggAvg:
		fn = sql.Avg
	case AggSum:
		fn = sql.Sum
	case AggMin:
		fn = sql.Min
	case AggMax:
		fn = sql.Max
	case AggCount:
		fn = sql.Count
	default:
		return nil, "", "", &QueryBuildError{
			Op:  "BaseAggregate.buildExpr",
			Err: fmt.Errorf("unsupported aggregate type %q", b.Type),
		}
	}

	if resolvedField == "*" {
		expr = "*"
	} else {
		if b.Distinct {
			expr = sql.Distinct(tbl.C(resolvedField))
		} else {
			expr = tbl.C(resolvedField)
		}
	}

	alias = b.Alias
	if alias == "" {
		prefix := strings.ToLower(string(b.Type))
		if b.Distinct {
			prefix += "_distinct"
		}
		safe := strings.ReplaceAll(b.Field, ".", "_")
		raw := fmt.Sprintf("%s_%s", prefix, safe)
		if len(raw) > 60 {
			raw = raw[:60]
		}
		alias = raw
	}

	return fn, expr, alias, nil
}

func (b *BaseAggregate) preprocess(filterCfg *FilterConfig, allowEmptyField bool) error {
	if b.Field == "" && !allowEmptyField {
		return &ValidationError{
			Rule: "AggregateFieldNotEmpty",
			Err:  fmt.Errorf("aggregate field must not be empty"),
		}
	}

	if b.Field != "" {
		parts, pos, ok := splitChain(b.Field)
		if !ok {
			return &ValidationError{
				Rule: "AggregateFieldSyntax",
				Err:  fmt.Errorf("invalid empty segment at char %d in %q", pos, b.Field),
			}
		}
		b.fieldParts = parts
	}

	switch b.Type {
	case AggCount, AggSum, AggAvg, AggMin, AggMax:
	default:
		return &ValidationError{
			Rule: "AggregateTypeUnsupported",
			Err:  fmt.Errorf("unsupported aggregate type %q", b.Type),
		}
	}

	if b.Distinct && !(b.Type == AggCount || b.Type == AggSum || b.Type == AggAvg) {
		return &ValidationError{
			Rule: "AggregateDistinctNotAllowed",
			Err:  fmt.Errorf("DISTINCT not supported for aggregate type %q", b.Type),
		}
	}

	if filterCfg != nil {
		if err := b.Filters.ValidateAndPreprocess(filterCfg); err != nil {
			return err
		}
	}

	b.preprocessed = true
	return nil
}

func applyBridgesInverseJoins(sel *sql.Selector, bridges []Bridge, base *sql.SelectTable) (*sql.SelectTable, error) {
	prev := base
	for i := len(bridges) - 1; i >= 1; i-- {
		joins := bridges[i].Inverse().Join(sel, prev)
		prev = joins[0]
	}
	return prev, nil
}

type Aggregate struct {
	BaseAggregate
}

func (a *Aggregate) Predicate(root Node) (func(*sql.Selector), string, error) {
	if !a.preprocessed {
		panic("Aggregate.Predicate: called before preprocess")
	}

	node, finalField, bridges, err := resolveChain(root, a.fieldParts)
	if err != nil {
		return nil, "", &QueryBuildError{
			Op:  "Aggregate.Predicate",
			Err: err,
		}
	}

	tbl := sql.Table(node.Table()).As("t0")
	fn, expr, alias, err := a.BaseAggregate.buildExpr(tbl, finalField)
	if err != nil {
		return nil, "", err
	}
	a.Alias = alias

	preds, err := a.BaseAggregate.Filters.Predicate(node)
	if err != nil {
		return nil, "", err
	}

	modifier := func(s *sql.Selector) {
		// apply policy only if nested
		if len(bridges) > 0 {
			if pol := bridges[len(bridges)-1].Child().Policy(); pol != nil {
				if err := pol.EvalQuery(s.Context(), nil); err != nil {
					s.AddError(err)
					return
				}
			}
		}

		sub := sql.Dialect(s.Dialect()).Select(fn(expr)).From(tbl)
		last, err := applyBridgesInverseJoins(sub, bridges, tbl)
		if err != nil {
			s.AddError(err)
			return
		}

		for _, p := range preds {
			p(sub)
		}
		// if no bridges, link on primary key
		if len(bridges) > 0 {
			relInfo := bridges[0].RelInfos()
			sub.Where(
				sql.ColumnsEQ(
					s.C(relInfo.FinalLeftField),
					last.C(relInfo.FinalRightField),
				),
			)
		} else {
			for _, f := range node.PKs() {
				sub.Where(
					sql.ColumnsEQ(s.C(f.StorageName), tbl.C(f.StorageName)),
				)
			}
		}
		s.AppendSelectExprAs(sub, alias)
	}
	return modifier, alias, nil
}

type Aggregates []*Aggregate

func (as Aggregates) Predicate(node Node) ([]func(*sql.Selector), []string, error) {
	lenAggregates := len(as)
	if lenAggregates == 0 {
		return nil, nil, nil
	}

	var (
		metaFields = make([]string, 0, lenAggregates)
		appliesAgg = make([]func(*sql.Selector), 0, lenAggregates)
	)
	for _, a := range as {
		apply, field, err := a.Predicate(node)
		if err != nil {
			return nil, nil, err
		}

		appliesAgg = append(appliesAgg, apply)
		metaFields = append(metaFields, field)
	}
	return appliesAgg, metaFields, nil
}

func (ags Aggregates) ValidateAndPreprocess(cfg *AggregateConfig) error {
	if cfg == nil {
		cfg = &AggregateConfig{}
	}
	for _, agg := range ags {
		if err := agg.ValidateAndPreprocess(cfg); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregate) ValidateAndPreprocess(cfg *AggregateConfig) error {
	if err := a.BaseAggregate.preprocess(cfg.FilterConfig, true); err != nil {
		return err
	}

	depth := len(a.fieldParts) - 1
	if cfg.MaxAggregateRelationDepth > 0 && depth > cfg.MaxAggregateRelationDepth {
		return &ValidationError{
			Rule: "MaxAggregateRelationsDepth",
			Err:  fmt.Errorf("aggregate relation depth of %d exceeds max %d", depth, cfg.MaxAggregateRelationDepth),
		}
	}
	return nil
}

type OverallAggregate struct {
	BaseAggregate
}

func (a *OverallAggregate) resolveField(registry Graph) (node Node, field string, err error) {
	node = registry[a.fieldParts[0]]
	if node == nil {
		err = &QueryBuildError{
			Op:  "OverallAggregate.resolveField",
			Err: fmt.Errorf("node named \"%s\" don't exist", a.fieldParts[0]),
		}
		return
	}

	if len(a.fieldParts) == 2 {
		if f := node.FieldByName(a.fieldParts[1]); f != nil {
			field = f.StorageName
		} else {
			err = &QueryBuildError{
				Op:  "OverallAggregate.resolveField",
				Err: fmt.Errorf("node \"%s\" don't have field named \"%s\"", node.Name(), a.fieldParts[1]),
			}
		}
	}
	return
}

// Build constructs a standalone selector for the overall aggregate.
func (a *OverallAggregate) Build(graph Graph) (*sql.Selector, string, error) {
	if !a.preprocessed {
		panic("OverallAggregate.Build: called before preprocess")
	}

	node, field, err := a.resolveField(graph)
	if err != nil {
		return nil, "", err
	}

	tbl := sql.Table(node.Table()).As("t0")
	fn, expr, alias, err := a.BaseAggregate.buildExpr(tbl, field)
	if err != nil {
		return nil, "", err
	}
	a.Alias = alias

	sel := (&sql.Selector{}).From(tbl)
	if pol := node.Policy(); pol != nil {
		if err := pol.EvalQuery(sel.Context(), nil); err != nil {
			return nil, "", err
		}
	}

	if preds, err := a.BaseAggregate.Filters.Predicate(node); err != nil {
		return nil, "", err
	} else if len(preds) > 0 {
		for _, p := range preds {
			p(sel)
		}
	}

	sel = sel.Select(fn(expr)).As(alias)
	return sel, alias, nil
}

func (a *OverallAggregate) BuildScalar(graph Graph) (*ScalarQuery, error) {
	sel, alias, err := a.Build(graph)
	if err != nil {
		return nil, err
	}
	sq := &ScalarQuery{
		Selector: sel,
		Key:      alias,
	}
	if a.Type == AggCount {
		sq.Dest = new(sql.NullInt64)
	} else {
		sq.Dest = new(sql.NullFloat64)
	}
	return sq, nil
}

func (oa *OverallAggregate) ValidateAndPreprocess(cfg *Config) error {
	if err := oa.BaseAggregate.preprocess(&cfg.FilterConfig, false); err != nil {
		return err
	}

	n := len(oa.fieldParts)
	if n < 1 || n > 2 {
		return &ValidationError{
			Rule: "OverallAggregateFieldFormat",
			Err:  fmt.Errorf("overall aggregate field %q must be [entity] or [entity.field]", oa.Field),
		}
	}
	return nil
}

type AggregatesResponse = map[string]any

type OverallAggregates []*OverallAggregate

func (oas OverallAggregates) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (AggregatesResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	err, count := oas.ValidateAndPreprocessFinal(cfg), len(oas)
	if err != nil || count == 0 {
		return nil, err
	}

	scalars, err := oas.BuildScalars(graph)
	if err != nil {
		return nil, err
	}

	chunks := splitInChunks(scalars, cfg.ScalarQueriesChunkSize)

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(min(len(chunks), cfg.MaxParallelWorkersPerRequest))

	res := ExecuteScalarGroupsAsync(wgctx, wg, client, cfg, count, chunks...)

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

func (oas OverallAggregates) BuildScalars(graph Graph) ([]*ScalarQuery, error) {
	if length := len(oas); length > 0 {
		var scalars = make([]*ScalarQuery, 0, length)

		for i, oa := range oas {
			s, err := oa.BuildScalar(graph)
			if err != nil {
				return nil, err
			}
			scalars[i] = s
		}
	}
	return nil, nil
}

func (oas OverallAggregates) ValidateAndPreprocessFinal(cfg *Config) error {
	count, err := oas.ValidateAndPreprocess(cfg)
	if err != nil {
		return err
	}

	return checkMaxAggregates(cfg, count)
}

func (oas OverallAggregates) ValidateAndPreprocess(cfg *Config) (count int, err error) {
	for _, oa := range oas {
		if err = oa.ValidateAndPreprocess(cfg); err != nil {
			return
		}
		count++
	}
	return
}
