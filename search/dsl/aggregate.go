package dsl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
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

var (
	ErrDistinctWithoutField = "cannot use DISTINCT with wildcard '*'; specify a column"
	ErrAggWithoutField      = "aggregate %q on '*' is invalid; only COUNT(*) is allowed"
	ErrUnsupportedAggType   = "unsupported aggregate type %q"
)

// buildExpr builds the aggregate function, expression, and alias.
func (b *BaseAggregate) buildExpr(tbl *sql.SelectTable, resolvedField string) (
	fn func(string) string, expr string, alias string, err error,
) {
	if !b.preprocessed {
		panic("BaseAggregate.buildExpr: called before preprocess")
	}

	if resolvedField == "" {
		if b.Distinct {
			return nil, "", "", &common.QueryBuildError{
				Op:  "BaseAggregate.buildExpr",
				Err: errors.New(ErrDistinctWithoutField),
			}
		}
		if b.Type != AggCount {
			return nil, "", "", &common.QueryBuildError{
				Op:  "BaseAggregate.buildExpr",
				Err: fmt.Errorf(ErrAggWithoutField, b.Type),
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
		return nil, "", "", &common.QueryBuildError{
			Op:  "BaseAggregate.buildExpr",
			Err: fmt.Errorf(ErrUnsupportedAggType, b.Type),
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
		alias = fmt.Sprintf("%s_%s", prefix, safe)
	}

	return fn, expr, alias, nil
}

func (b *BaseAggregate) preprocess(filterCfg *common.FilterConfig, allowEmptyField bool) error {
	if b.Field == "" && !allowEmptyField {
		return &common.ValidationError{
			Rule: "AggregateFieldNotEmpty",
			Err:  fmt.Errorf("aggregate field must not be empty"),
		}
	}

	if b.Field != "" {
		parts, pos, ok := splitChain(b.Field)
		if !ok {
			return &common.ValidationError{
				Rule: "AggregateFieldSyntax",
				Err:  fmt.Errorf("invalid empty segment at char %d in %q", pos, b.Field),
			}
		}
		b.fieldParts = parts
	}

	switch b.Type {
	case AggCount, AggSum, AggAvg, AggMin, AggMax:
	default:
		return &common.ValidationError{
			Rule: "AggregateTypeUnsupported",
			Err:  fmt.Errorf("unsupported aggregate type %q", b.Type),
		}
	}

	if b.Distinct && !(b.Type == AggCount || b.Type == AggSum || b.Type == AggAvg) {
		return &common.ValidationError{
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

func applyBridgesInverseJoins(sel *sql.Selector, bridges []entx.Bridge, base *sql.SelectTable) *sql.SelectTable {
	prev := base
	for i := len(bridges) - 1; i >= 1; i-- {
		joins := bridges[i].Inverse().Join(sel, prev)
		prev = joins[0]
	}
	return prev
}

type Aggregate struct {
	BaseAggregate
}

func (a *Aggregate) Predicate(ctx context.Context, root entx.Node, dialect string) (func(*sql.Selector), string, error) {
	if !a.preprocessed {
		panic("Aggregate.Predicate: called before preprocess")
	}

	node, finalField, bridges, err := resolveChain(root, a.fieldParts)
	if err != nil {
		return nil, "", &common.QueryBuildError{
			Op:  "Aggregate.Predicate",
			Err: err,
		}
	}

	var preds []func(*sql.Selector)

	// apply policy only on the last nested node
	if len(bridges) > 0 {
		policyPred, err := common.EnforcePolicy(ctx, bridges[len(bridges)-1].Child(), common.OpAggregate)
		if err != nil {
			return nil, "", err
		}
		if policyPred != nil {
			preds = append(preds, policyPred)
		}
	}

	tbl := sql.Table(node.Table()).As("t0")
	fn, expr, alias, err := a.BaseAggregate.buildExpr(tbl, finalField)
	if err != nil {
		return nil, "", err
	}
	a.Alias = alias

	filtersPreds, err := a.BaseAggregate.Filters.Predicate(node)
	if err != nil {
		return nil, "", err
	}
	preds = append(preds, filtersPreds...)

	sub := sql.Dialect(dialect).Select(fn(expr)).From(tbl)
	last := applyBridgesInverseJoins(sub, bridges, tbl)

	for _, p := range preds {
		p(sub)
	}

	modifier := func(s *sql.Selector) {
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

func (as Aggregates) Predicate(ctx context.Context, root entx.Node, dialect string) ([]func(*sql.Selector), []string, error) {
	lenAggregates := len(as)
	if lenAggregates == 0 {
		return nil, nil, nil
	}

	var (
		metaFields = make([]string, 0, lenAggregates)
		appliesAgg = make([]func(*sql.Selector), 0, lenAggregates)
	)
	for _, a := range as {
		apply, field, err := a.Predicate(ctx, root, dialect)
		if err != nil {
			return nil, nil, err
		}

		appliesAgg = append(appliesAgg, apply)
		metaFields = append(metaFields, field)
	}
	return appliesAgg, metaFields, nil
}

func (ags Aggregates) ValidateAndPreprocess(cfg *common.AggregateConfig) error {
	if cfg == nil {
		cfg = &common.AggregateConfig{}
	}
	for _, agg := range ags {
		if err := agg.ValidateAndPreprocess(cfg); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregate) ValidateAndPreprocess(cfg *common.AggregateConfig) error {
	if err := a.BaseAggregate.preprocess(cfg.FilterConfig, true); err != nil {
		return err
	}

	depth := len(a.fieldParts) - 1
	if cfg.MaxAggregateRelationDepth > 0 && depth > cfg.MaxAggregateRelationDepth {
		return &common.ValidationError{
			Rule: "MaxAggregateRelationsDepth",
			Err:  fmt.Errorf("aggregate relation depth of %d exceeds max %d", depth, cfg.MaxAggregateRelationDepth),
		}
	}
	return nil
}

var (
	ErrNodeNotExist     = "node named '%s' don't exist"
	ErrNodeNotHaveField = "node '%s' don't have field named '%s'"
)

type OverallAggregate struct {
	BaseAggregate
}

func (a *OverallAggregate) resolveField(registry entx.Graph) (node entx.Node, field string, err error) {
	node = registry[a.fieldParts[0]]
	if node == nil {
		err = &common.QueryBuildError{
			Op:  "OverallAggregate.resolveField",
			Err: fmt.Errorf(ErrNodeNotExist, a.fieldParts[0]),
		}
		return
	}

	if len(a.fieldParts) == 2 {
		if f := node.FieldByName(a.fieldParts[1]); f != nil {
			field = f.StorageName
		} else {
			err = &common.QueryBuildError{
				Op:  "OverallAggregate.resolveField",
				Err: fmt.Errorf(ErrNodeNotHaveField, node.Name(), a.fieldParts[1]),
			}
		}
	}
	return
}

// Build constructs a standalone selector for the overall aggregate.
func (a *OverallAggregate) Build(ctx context.Context, graph entx.Graph, dialect string) (*sql.Selector, string, error) {
	if !a.preprocessed {
		panic("OverallAggregate.Build: called before preprocess")
	}

	node, field, err := a.resolveField(graph)
	if err != nil {
		return nil, "", err
	}

	policyPred, err := common.EnforcePolicy(ctx, node, common.OpAggregateOverall)
	if err != nil {
		return nil, "", err
	}

	tbl := sql.Table(node.Table()).As("t0")
	fn, expr, alias, err := a.BaseAggregate.buildExpr(tbl, field)
	if err != nil {
		return nil, "", err
	}
	a.Alias = alias

	sel := sql.Dialect(dialect).Select(fn(expr)).As(alias).From(tbl)
	if policyPred != nil {
		policyPred(sel)
	}
	if preds, err := a.BaseAggregate.Filters.Predicate(node); err != nil {
		return nil, "", err
	} else if len(preds) > 0 {
		for _, p := range preds {
			p(sel)
		}
	}

	return sel, alias, nil
}

func (a *OverallAggregate) BuildScalar(ctx context.Context, graph entx.Graph, dialect string) (*common.ScalarQuery, error) {
	sel, alias, err := a.Build(ctx, graph, dialect)
	if err != nil {
		return nil, err
	}
	sq := &common.ScalarQuery{
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

func (oa *OverallAggregate) ValidateAndPreprocess(cfg *common.Config) error {
	if err := oa.BaseAggregate.preprocess(&cfg.FilterConfig, false); err != nil {
		return err
	}

	n := len(oa.fieldParts)
	if n < 1 || n > 2 {
		return &common.ValidationError{
			Rule: "OverallAggregateFieldFormat",
			Err:  fmt.Errorf("overall aggregate field %q must be [entity] or [entity.field]", oa.Field),
		}
	}
	return nil
}

type OverallAggregates []*OverallAggregate

func (oas OverallAggregates) Execute(
	ctx context.Context,
	client entx.Client,
	graph entx.Graph,
	cfg *common.Config,
) (common.AggregatesResponse, error) {
	ctx, cancel := common.ContextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	err, count := oas.ValidateAndPreprocessFinal(cfg), len(oas)
	if err != nil || count == 0 {
		return nil, err
	}

	scalars, err := oas.BuildScalars(ctx, graph, cfg.Dialect)
	if err != nil {
		return nil, err
	}

	chunks := common.SplitInChunks(scalars, cfg.ScalarQueriesChunkSize)

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(min(len(chunks), cfg.MaxParallelWorkersPerRequest))

	res := common.NewMapSync(make(map[string]any, count))
	common.ExecuteScalarGroupsAsync(wgctx, wg, client, cfg, res, chunks...)

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	return res.UnsafeRaw(), nil
}

func (oas OverallAggregates) BuildScalars(ctx context.Context, graph entx.Graph, dialect string) ([]*common.ScalarQuery, error) {
	if length := len(oas); length > 0 {
		var scalars = make([]*common.ScalarQuery, length)

		for i, oa := range oas {
			s, err := oa.BuildScalar(ctx, graph, dialect)
			if err != nil {
				return nil, err
			}
			scalars[i] = s
		}

		return scalars, nil
	}
	return nil, nil
}

func (oas OverallAggregates) ValidateAndPreprocessFinal(cfg *common.Config) error {
	count, err := oas.ValidateAndPreprocess(cfg)
	if err != nil {
		return err
	}

	return common.CheckMaxAggregates(cfg, count)
}

func (oas OverallAggregates) ValidateAndPreprocess(cfg *common.Config) (count int, err error) {
	for _, oa := range oas {
		if err = oa.ValidateAndPreprocess(cfg); err != nil {
			return
		}
		count++
	}
	return
}
