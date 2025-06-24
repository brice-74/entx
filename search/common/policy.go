package common

import (
	"context"
	"errors"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/privacy"
	"github.com/brice-74/entx"
)

type QueryModifier struct {
	Op       QueryOp
	Modifier func(*sql.Selector)
}

type QueryOp string

const (
	OpAggregate        QueryOp = "Aggregate"
	OpAggregateOverall QueryOp = "AggregateOverall"
	OpCountPaginate    QueryOp = "CountPaginate"
	OpRootQuery        QueryOp = "RootQuery"
	OpIncludeQuery     QueryOp = "IncludeQuery"
	OpLastIncludeQuery QueryOp = "IncludeQuery"
)

type QueryPolicy struct {
	// Enforcer is called during build phases of:
	// paginate count | aggregate | overall agreggate | base query | include
	Enforcer func(context.Context, QueryOp) (func(*sql.Selector), error)
}

type policyToken struct{}

func ContextWithPolicyToken(ctx context.Context) context.Context {
	return context.WithValue(ctx, policyToken{}, struct{}{})
}

func IsPolicyTokenPresent(ctx context.Context) bool {
	_, ok := ctx.Value(policyToken{}).(struct{})
	return ok
}

func (p QueryPolicy) EvalQuery(ctx context.Context, q ent.Query) error {
	if !IsPolicyTokenPresent(ctx) {
		return privacy.Skip
	}

	switch t := q.(type) {
	case *QueryModifier:
		if p.Enforcer != nil {
			switch modifier, decision := p.Enforcer(ctx, t.Op); {
			case decision != nil &&
				!errors.Is(decision, privacy.Skip) &&
				!errors.Is(decision, privacy.Allow):
				return decision
			default:
				t.Modifier = modifier
			}
		}
		return privacy.Allow
	default:
		return privacy.Allow
	}
}

func EnforcePolicy(ctx context.Context, node entx.Node, op QueryOp) (func(*sql.Selector), error) {
	m := QueryModifier{
		Op: op,
	}
	if err := node.Policy().EvalQuery(
		ctx,
		&m,
	); err != nil {
		return nil, err
	}
	return m.Modifier, nil
}
