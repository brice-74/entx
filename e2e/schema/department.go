package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Department struct {
	ent.Schema
}

func (Department) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id").Unique(),
		field.String("name"),
	}
}

func (Department) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("employees", Employee.Type),
	}
}
