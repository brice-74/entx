package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Article struct {
	ent.Schema
}

func (Article) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id").Unique(),
		field.Int("user_id"),
		field.String("title"),
		field.String("content").Optional(),
		field.Bool("published").Default(false),
		field.Time("created_at").Default(time.Now),
	}
}

func (Article) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("author", User.Type).Field("user_id").Ref("articles").Unique().Required(),
		edge.To("comments", Comment.Type),
		edge.From("tags", Tag.Type).
			Ref("articles").
			Through("article_tag", ArticleTag.Type),
	}
}
