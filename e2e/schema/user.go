package schema

import (
	"context"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/privacy"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/brice-74/entx/search"
)

type User struct {
	ent.Schema
}

func (User) Fields() []ent.Field {
	return []ent.Field{
		field.String("name"),
		field.String("email").Unique(),
		field.Int("age").Optional(),
		field.Bool("is_active").Default(true),
		field.Time("created_at").Default(time.Now),
		field.Time("updated_at").Default(time.Now),
	}
}

func (User) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("articles", Article.Type),
		edge.To("comments", Comment.Type),
		edge.To("employee", Employee.Type).Unique(),
	}
}

func (User) Policy() ent.Policy {
	return privacy.Policy{
		Query: privacy.QueryPolicy{
			search.QueryPolicy{
				Enforcer: func(ctx context.Context, qo search.QueryOp) (func(*sql.Selector), error) {
					return nil, nil
				},
			},
			OtherQueryPolicy{},
		},
		Mutation: privacy.MutationPolicy{
			OtherMutatePolicy{},
		},
	}
}

type OtherQueryPolicy struct{}

func (OtherQueryPolicy) EvalQuery(ctx context.Context, q ent.Query) error {
	return privacy.Allow
}

type OtherMutatePolicy struct{}

func (OtherMutatePolicy) EvalMutation(context.Context, ent.Mutation) error {
	return privacy.Allow
}
