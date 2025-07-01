package common

import (
	"database/sql"
	"time"

	"entgo.io/ent/dialect"
)

type SortConfig struct {
	// MaxSortRelationDepth is the maximum allowed nesting depth for sorting fields
	// (number of relationship hops + field segments).
	MaxSortRelationDepth int
}

type PageableConfig struct {
	// MaxLimit is the maximum number of items allowed per page.
	MaxLimit int
	// DefaultLimit is the default number of items per page if none is specified.
	DefaultLimit int
}

type IncludeConfig struct {
	// MaxIncludeTreeCount is the total number of Include nodes allowed in one include tree.
	MaxIncludeTreeCount int
	// MaxIncludeRelationDepth is the maximum depth of the relation chain allowed per Include.
	MaxIncludeRelationDepth int
	*FilterConfig
	*AggregateConfig
	*PageableConfig
}

type AggregateConfig struct {
	// MaxAggregateRelationDepth is the maximum depth (field chain segments)
	// allowed for an aggregate’s target field.
	MaxAggregateRelationDepth int
	*FilterConfig
}

// FilterConfig defines limits on filtering expressions.
type FilterConfig struct {
	// MaxFilterTreeCount is the maximum number of Filter nodes allowed in a single filter tree.
	MaxFilterTreeCount int
	// MaxRelationChainDepth is the maximum depth allowed per filter,
	// counting both relation segments and field segments.
	MaxRelationChainDepth int
	// MaxRelationTotalCount is the total number of relation segments permitted
	// across the entire filter tree.
	MaxRelationTotalCount int
}

type Option func(*Config)

// defaultConf is the configuration with the fewest restrictions to make the hub functional.
var defaultConf = Config{
	Dialect: dialect.MySQL,
	Transaction: TransactionConfig{
		EnablePaginateQuery:       true,
		AllowClientIsolationLevel: true,
	},
	PageableConfig: PageableConfig{
		MaxLimit:     100,
		DefaultLimit: 25,
	},
	ScalarQueriesChunkSize:       7,
	MaxParallelWorkersPerRequest: -1,
}

var DefaultConf = *defaultConf.BindDeeply()

type TransactionConfig struct {
	IsolationLevel      sql.IsolationLevel
	EnablePaginateQuery bool
	// Client input permissions
	AllowClientIsolationLevel bool
}

type Config struct {
	Dialect        string
	Transaction    TransactionConfig
	RequestTimeout time.Duration
	// Batch sizing
	ScalarQueriesChunkSize       int
	MaxParallelWorkersPerRequest int
	// Validations
	MaxAggregatesPerRequest int
	MaxSearchesPerRequest   int
	PageableConfig
	SortConfig
	FilterConfig
	IncludeConfig
	AggregateConfig
}

func NewConfig(opts ...Option) *Config {
	cfg := defaultConf
	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg.BindDeeply()
}

func (cfg *Config) BindDeeply() *Config {
	cfg.IncludeConfig.AggregateConfig = &cfg.AggregateConfig
	cfg.IncludeConfig.FilterConfig = &cfg.FilterConfig
	cfg.IncludeConfig.PageableConfig = &cfg.PageableConfig
	cfg.AggregateConfig.FilterConfig = &cfg.FilterConfig
	return cfg
}

func WithTransactionConfig(cfg TransactionConfig) Option {
	return func(c *Config) {
		c.Transaction = cfg
	}
}

// ------------------------------
// Timeouts
// ------------------------------

// WithRequestTimeout sets the global request timeout.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.RequestTimeout = d
	}
}

// ------------------------------
// BatchSizing
// ------------------------------

// WithScalarQueriesChunkSize sets the chunk size for batching scalar queries.
func WithScalarQueriesChunkSize(size int) Option {
	return func(c *Config) {
		c.ScalarQueriesChunkSize = size
	}
}

// WithMaxParallelWorkersPerRequest sets the maximum number of parallel workers per request.
func WithMaxParallelWorkersPerRequest(count int) Option {
	return func(c *Config) {
		c.MaxParallelWorkersPerRequest = count
	}
}

// ------------------------------
// Validation
// ------------------------------

// WithMaxAggregatesPerRequest sets the maximum number of aggregates per request.
func WithMaxAggregatesPerRequest(max int) Option {
	return func(c *Config) {
		c.MaxAggregatesPerRequest = max
	}
}

// WithMaxSearchesPerRequest sets the maximum number of searches per request.
func WithMaxSearchesPerRequest(max int) Option {
	return func(c *Config) {
		c.MaxSearchesPerRequest = max
	}
}

func WithPageableConfig(cfg PageableConfig) Option {
	return func(c *Config) {
		c.PageableConfig = cfg
	}
}

func WithSortConfig(cfg SortConfig) Option {
	return func(c *Config) {
		c.SortConfig = cfg
	}
}

func WithFilterConfig(cfg FilterConfig) Option {
	return func(c *Config) {
		c.FilterConfig = cfg
	}
}

func WithIncludeConfig(cfg IncludeConfig) Option {
	return func(c *Config) {
		c.IncludeConfig = cfg
	}
}

func WithAggregateConfig(cfg AggregateConfig) Option {
	return func(c *Config) {
		c.AggregateConfig = cfg
	}
}
