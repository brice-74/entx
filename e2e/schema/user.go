package schema

import (
	"context"
	"fmt"

	"entgo.io/ent"
	"entgo.io/ent/privacy"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
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
		field.Time("created_at"),
		field.Time("updated_at"),
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
			CustomPolicy{},
		},
	}
}

type CustomPolicy struct{}

func (CustomPolicy) EvalQuery(ctx context.Context, q ent.Query) error {
	fmt.Printf("%+v | %T\n", ctx.Value("aaa"), q)
	return privacy.Allow
}
