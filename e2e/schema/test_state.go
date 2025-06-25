package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"github.com/brice-74/entx/search/extension"
)

type TestState struct {
	ent.Schema
}

func (TestState) Fields() []ent.Field {
	return []ent.Field{
		field.String("key").Unique(),
		field.Bool("done"),
	}
}

func (TestState) Annotations() []schema.Annotation {
	return []schema.Annotation{
		extension.ExcludeNode(),
	}
}
