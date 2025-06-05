package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type ArticleTag struct {
	ent.Schema
}

func (ArticleTag) Fields() []ent.Field {
	return []ent.Field{
		field.Int("tag_id"),
		field.Int("article_id"),
	}
}

func (ArticleTag) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("article", Article.Type).
			Field("article_id").
			Unique().
			Required(),
		edge.To("tag", Tag.Type).
			Field("tag_id").
			Unique().
			Required(),
	}
}

func (ArticleTag) Annotations() []schema.Annotation {
	return []schema.Annotation{
		field.ID("tag_id", "article_id"),
	}
}
