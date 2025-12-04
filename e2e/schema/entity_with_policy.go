package schema

/* type EntityWithPolicy struct {
	ent.Schema
}

func (EntityWithPolicy) Fields() []ent.Field {
	return []ent.Field{}
}

func (EntityWithPolicy) Edges() []ent.Edge {
	return []ent.Edge{}
}

func (EntityWithPolicy) Policy() ent.Policy {
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
} */
