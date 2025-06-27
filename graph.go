package entx

import (
	"context"
	stdsql "database/sql"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
)

type (
	Graph = map[string]Node

	Transaction interface {
		Rollback() error
		Commit() error
	}

	Client interface {
		Debug() Client
		GetEntityClient(Node) (EntityClient, error)
		MustGetEntityClient(Node) EntityClient
		QueryContext(ctx context.Context, query string, args ...any) (*stdsql.Rows, error)
		Tx(ctx context.Context, opts *stdsql.TxOptions) (Transaction, Client, error)
	}

	EntityClient interface {
		Query() Query
	}

	EntityMeta struct {
		Aggregates map[string]any `json:"aggregates,omitempty"`
	}

	Entity interface {
		Metadatas() *EntityMeta
		// native ent method to retrieve additionnal query values
		Value(name string) (ent.Value, error)
	}

	Query interface {
		Predicate(...func(s *sql.Selector)) Query
		Select(columns ...string) Query
		All(ctx context.Context) ([]Entity, error)
	}

	Node interface {
		PKs() []*Field
		Name() string
		Table() string
		Policy() ent.Policy
		NewQuery(Client) Query
		FieldByName(s string) *Field
		Bridge(string) Bridge
	}

	Bridge interface {
		RelInfos() *RelationInfos
		JoinPivot(*sql.Selector, ...*sql.SelectTable) *sql.SelectTable
		Join(*sql.Selector, ...*sql.SelectTable) []*sql.SelectTable
		Include(parentQuery Query, childQuery func(Query), handlers ...EntityHandler) Query
		FilterWith(...func(*sql.Selector)) func(*sql.Selector)
		Filter() func(*sql.Selector)
		Inverse() Bridge
		Child() Node
		Parent() Node
	}

	RelationInfos struct {
		RelType sqlgraph.Rel
		// used by O2O, O2M, M2O and M2M relations
		FinalLeftField  string
		FinalRightField string
		// used only by M2M relation
		PivotTable      string
		PivotLeftField  string
		PivotRightField string
	}

	EntityHandler func(entities []Entity) error
)
