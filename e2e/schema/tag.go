package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Tag struct {
	ent.Schema
}

func (Tag) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id").Unique(),
		field.String("name").Unique(),
	}
}

func (Tag) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("articles", Article.Type).Through("article_tag", ArticleTag.Type),
	}
}
