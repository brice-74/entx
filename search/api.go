package search

import (
	"context"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"

	stdsql "database/sql"
)

type Graph = map[string]Node

type (
	Client interface {
		GetEntityClient(Node) (EntityClient, error)
		MustGetEntityClient(Node) EntityClient
		QueryContext(ctx context.Context, query string, args ...any) (*stdsql.Rows, error)
	}

	EntityClient interface {
		Query() Query
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
)

type RelationInfos struct {
	RelType sqlgraph.Rel
	// used by O2O, O2M, M2O and M2M relations
	FinalLeftField  string
	FinalRightField string
	// used only by M2M relation
	PivotTable      string
	PivotLeftField  string
	PivotRightField string
}

type Direction string

const (
	DirASC  Direction = "ASC"
	DirDESC Direction = "DESC"
)

type Operator string

const (
	OpEqual        Operator = "="
	OpNotEqual     Operator = "!="
	OpGreaterThan  Operator = ">"
	OpGreaterEqual Operator = ">="
	OpLessThan     Operator = "<"
	OpLessEqual    Operator = "<="
	OpLike         Operator = "LIKE"
	OpNotLike      Operator = "NOT LIKE"
	OpIn           Operator = "IN"
	OpNotIn        Operator = "NOT IN"
)

type Agg string

const (
	AggAvg   Agg = "avg"
	AggSum   Agg = "sum"
	AggMin   Agg = "min"
	AggMax   Agg = "max"
	AggCount Agg = "count"
)

type (
	// ------------------------------
	// Input Types
	// ------------------------------

	CompositeRequest struct {
		Searches   []NamedQuery       `json:"searches,omitempty"`
		Aggregates []OverallAggregate `json:"aggregates,omitempty"`
	}

	NamedQuery struct {
		Key string `json:"key"`
		TargetedQuery
	}

	TargetedQuery struct {
		From string `json:"from"`
		QueryOptions
	}

	QueryOptions struct {
		Select         Select     `json:"select,omitempty"`
		Filters        Filters    `json:"filters,omitempty"`
		Includes       Includes   `json:"includes,omitempty"`
		Sort           Sorts      `json:"sort,omitempty"`
		Aggregates     Aggregates `json:"aggregates,omitempty"`
		WithPagination bool       `json:"with_pagination,omitempty"`
		Pageable
	}

	Select []string

	Pageable struct {
		Page  int `json:"page,omitempty"`
		Limit int `json:"limit,omitempty"`
	}

	Sort struct {
		Field     string    `json:"field"`
		Direction Direction `json:"direction,omitempty"`
		Aggregate Agg       `json:"aggregate,omitempty"`
		// pre-processed segments
		fieldParts   []string
		preprocessed bool
	}

	Sorts []*Sort

	Filter struct {
		Not      *Filter  `json:"not,omitempty"`
		And      Filters  `json:"and,omitempty"`
		Or       Filters  `json:"or,omitempty"`
		Relation string   `json:"relation,omitempty"`
		Field    string   `json:"field,omitempty"`
		Operator Operator `json:"operator,omitempty"`
		Value    any      `json:"value,omitempty"`
		// pre-processed segments
		relationParts []string
		fieldParts    []string
		preprocessed  bool
	}

	Filters []Filter

	Include struct {
		Relation   string     `json:"relation"`
		Select     Select     `json:"select,omitempty"`
		Filters    Filters    `json:"filters,omitempty"`
		Includes   Includes   `json:"includes,omitempty"`
		Sort       Sorts      `json:"sort,omitempty"`
		Aggregates Aggregates `json:"aggregates,omitempty"`
		Pageable
		// pre-processed segments
		relationParts []string
		preprocessed  bool
	}

	Includes []Include

	BaseAggregate struct {
		Field    string  `json:"field"`
		Alias    string  `json:"alias,omitempty"`
		Type     Agg     `json:"type"`
		Distinct bool    `json:"distinct,omitempty"`
		Filters  Filters `json:"filters,omitempty"`
		// pre-processed segments
		fieldParts   []string
		preprocessed bool
	}

	Aggregate struct {
		BaseAggregate
	}

	Aggregates []Aggregate

	OverallAggregate struct {
		TransactionWith string `json:"transaction_with,omitempty"`
		ParralelGroup   string `json:"parralel_group,omitempty"`
		BaseAggregate
	}

	// ------------------------------
	// Output Types
	// ------------------------------

	PaginateResponse struct {
		From        int `json:"from"`
		To          int `json:"to"`
		Total       int `json:"total"`
		CurrentPage int `json:"current_page"`
		LastPage    int `json:"last_page"`
		PerPage     int `json:"per_page"`
	}

	AggregatesMeta struct {
		Aggregates map[string]any `json:"aggregates,omitempty"`
	}

	SearchMeta struct {
		AggregatesMeta
		Paginate *PaginateResponse `json:"paginate,omitempty"`
		Count    int               `json:"count,omitempty"`
	}

	EntityMeta           = AggregatesMeta
	GlobalAggregatesMeta = AggregatesMeta

	SearchResponse struct {
		Data any        `json:"data,omitempty"`
		Meta SearchMeta `json:"meta,omitempty"`
	}

	CompositeResponse struct {
		Searches map[string]*SearchResponse `json:"searches,omitempty"`
		Meta     GlobalAggregatesMeta       `json:"meta,omitempty"`
	}
)
