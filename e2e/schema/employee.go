package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Employee struct {
	ent.Schema
}

func (Employee) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id").Unique(),
		field.Time("hire_date").Default(time.Now),
		field.Int("manager_id").Optional(),
		field.Int("user_id"),
		field.Int("department_id"),
	}
}

func (Employee) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("user", User.Type).Field("user_id").Ref("employee").Unique().Required(),
		edge.From("department", Department.Type).Field("department_id").Ref("employees").Unique().Required(),
		edge.To("manager", Employee.Type).Field("manager_id").Unique(),
		edge.From("reports", Employee.Type).Ref("manager"),
	}
}
