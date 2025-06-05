package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Comment struct {
	ent.Schema
}

func (Comment) Fields() []ent.Field {
	return []ent.Field{
		field.String("body"),
		field.Time("created_at"),
		field.Int("user_id"),
		field.Int("article_id"),
	}
}

func (Comment) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("user", User.Type).Field("user_id").Ref("comments").Unique().Required(),
		edge.From("article", Article.Type).Field("article_id").Ref("comments").Unique().Required(),
	}
}
