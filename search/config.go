package search

import "time"

type PageableConfig struct {
	// MaxLimit is the maximum number of items allowed per page.
	MaxLimit int
	// DefaultLimit is the default number of items per page if none is specified.
	DefaultLimit int
}

type SortConfig struct {
	// MaxSortRelationDepth is the maximum allowed nesting depth for sorting fields
	// (number of relationship hops + field segments).
	MaxSortRelationDepth int
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

type Option func(*Config)

// defaultConf is the configuration with the fewest restrictions to make the hub functional.
var defaultConf = Config{
	PageableConfig: PageableConfig{
		MaxLimit:     100,
		DefaultLimit: 25,
	},
	ScalarQueriesChunkSize:       5,
	MaxParallelWorkersPerRequest: -1,
}

var DefaultConf = *defaultConf.BindDeeply()

type Config struct {
	// Timeouts
	RequestTimeout     time.Duration
	SearchQueryTimeout time.Duration
	ScalarQueryTimeout time.Duration
	// Batch sizing
	ScalarQueriesChunkSize       int
	MaxParallelWorkersPerRequest int
	// Validation
	MaxAggregatesPerRequest int
	MaxSearchesPerRequest   int
	PageableConfig
	SortConfig
	FilterConfig
	IncludeConfig
	AggregateConfig
}

func NewConfig(opts ...Option) Config {
	cfg := defaultConf
	for _, opt := range opts {
		opt(&cfg)
	}

	return *cfg.BindDeeply()
}

func (cfg *Config) BindDeeply() *Config {
	cfg.IncludeConfig.AggregateConfig = &cfg.AggregateConfig
	cfg.IncludeConfig.FilterConfig = &cfg.FilterConfig
	cfg.IncludeConfig.PageableConfig = &cfg.PageableConfig
	cfg.AggregateConfig.FilterConfig = &cfg.FilterConfig
	return cfg
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

// WithSearchQueryTimeout sets the timeout for search queries.
func WithSearchQueryTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.SearchQueryTimeout = d
	}
}

// WithScalarQueryTimeout sets the timeout for scalar queries.
func WithScalarQueryTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.ScalarQueryTimeout = d
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

// ------------------------------
// PageableConfig
// ------------------------------

// WithMaxLimit sets the maximum number of items allowed per page.
func WithMaxLimit(max int) Option {
	return func(c *Config) {
		c.PageableConfig.MaxLimit = max
	}
}

// WithDefaultLimit sets the default number of items per page if none is specified.
func WithDefaultLimit(def int) Option {
	return func(c *Config) {
		c.PageableConfig.DefaultLimit = def
	}
}

// ------------------------------
// SortConfig
// ------------------------------

// WithMaxSortRelationDepth sets the maximum allowed nesting depth for sorting fields.
func WithMaxSortRelationDepth(depth int) Option {
	return func(c *Config) {
		c.SortConfig.MaxSortRelationDepth = depth
	}
}

// ------------------------------
// FilterConfig
// ------------------------------

// WithMaxFilterTreeCount sets the maximum number of Filter nodes allowed in a single filter tree.
func WithMaxFilterTreeCount(count int) Option {
	return func(c *Config) {
		c.FilterConfig.MaxFilterTreeCount = count
	}
}

// WithMaxRelationChainDepth sets the maximum depth allowed per filter (relation+field).
func WithMaxRelationChainDepth(depth int) Option {
	return func(c *Config) {
		c.FilterConfig.MaxRelationChainDepth = depth
	}
}

// WithMaxRelationTotalCount sets the total number of relation segments permitted across the entire filter tree.
func WithMaxRelationTotalCount(count int) Option {
	return func(c *Config) {
		c.FilterConfig.MaxRelationTotalCount = count
	}
}

// ------------------------------
// AggregateConfig
// ------------------------------

// WithMaxAggregateRelationDepth sets the maximum depth (field chain segments) allowed for an aggregate’s target field.
func WithMaxAggregateRelationDepth(depth int) Option {
	return func(c *Config) {
		c.AggregateConfig.MaxAggregateRelationDepth = depth
	}
}

// WithAggregateFilterMaxTreeCount sets the maximum Filter nodes allowed inside an Aggregate.
func WithAggregateFilterMaxTreeCount(count int) Option {
	return func(c *Config) {
		c.AggregateConfig.FilterConfig.MaxFilterTreeCount = count
	}
}

// WithAggregateFilterMaxRelationChainDepth sets the maximum (relation+field) depth inside an Aggregate’s filters.
func WithAggregateFilterMaxRelationChainDepth(depth int) Option {
	return func(c *Config) {
		c.AggregateConfig.FilterConfig.MaxRelationChainDepth = depth
	}
}

// WithAggregateFilterMaxRelationTotalCount sets the total relation segments allowed in Aggregate’s filter tree.
func WithAggregateFilterMaxRelationTotalCount(count int) Option {
	return func(c *Config) {
		c.AggregateConfig.FilterConfig.MaxRelationTotalCount = count
	}
}

// ------------------------------
// IncludeConfig
// ------------------------------

// WithMaxIncludeTreeCount sets the total number of Include nodes allowed in one include tree.
func WithMaxIncludeTreeCount(count int) Option {
	return func(c *Config) {
		c.IncludeConfig.MaxIncludeTreeCount = count
	}
}

// WithMaxIncludeRelationDepth sets the maximum depth of the relation chain allowed per Include.
func WithMaxIncludeRelationDepth(depth int) Option {
	return func(c *Config) {
		c.IncludeConfig.MaxIncludeRelationDepth = depth
	}
}

// WithIncludeFilterMaxTreeCount sets the maximum Filter nodes allowed inside any Include.
func WithIncludeFilterMaxTreeCount(count int) Option {
	return func(c *Config) {
		c.IncludeConfig.FilterConfig.MaxFilterTreeCount = count
	}
}

// WithIncludeFilterMaxRelationChainDepth sets the maximum (relation+field) depth for filters inside Includes.
func WithIncludeFilterMaxRelationChainDepth(depth int) Option {
	return func(c *Config) {
		c.IncludeConfig.FilterConfig.MaxRelationChainDepth = depth
	}
}

// WithIncludeFilterMaxRelationTotalCount sets the total relation segments allowed in Include’s filter trees.
func WithIncludeFilterMaxRelationTotalCount(count int) Option {
	return func(c *Config) {
		c.IncludeConfig.FilterConfig.MaxRelationTotalCount = count
	}
}

// WithIncludeAggregateMaxRelationDepth sets the maximum field-chain depth for aggregates inside Includes.
func WithIncludeAggregateMaxRelationDepth(depth int) Option {
	return func(c *Config) {
		c.IncludeConfig.AggregateConfig.MaxAggregateRelationDepth = depth
	}
}

// WithIncludeAggregateFilterMaxTreeCount sets the maximum Filter nodes allowed in aggregates inside Includes.
func WithIncludeAggregateFilterMaxTreeCount(count int) Option {
	return func(c *Config) {
		c.IncludeConfig.AggregateConfig.FilterConfig.MaxFilterTreeCount = count
	}
}

// WithIncludeAggregateFilterMaxRelationChainDepth sets the max (relation+field) depth for filters inside Include’s aggregates.
func WithIncludeAggregateFilterMaxRelationChainDepth(depth int) Option {
	return func(c *Config) {
		c.IncludeConfig.AggregateConfig.FilterConfig.MaxRelationChainDepth = depth
	}
}

// WithIncludeAggregateFilterMaxRelationTotalCount sets the total relation segments allowed in Include’s aggregate filters.
func WithIncludeAggregateFilterMaxRelationTotalCount(count int) Option {
	return func(c *Config) {
		c.IncludeConfig.AggregateConfig.FilterConfig.MaxRelationTotalCount = count
	}
}

// WithIncludePageableMaxLimit sets the maximum limit for pagination inside Includes.
func WithIncludePageableMaxLimit(max int) Option {
	return func(c *Config) {
		c.IncludeConfig.PageableConfig.MaxLimit = max
	}
}

// WithIncludePageableDefaultLimit sets the default limit for pagination inside Includes.
func WithIncludePageableDefaultLimit(def int) Option {
	return func(c *Config) {
		c.IncludeConfig.PageableConfig.DefaultLimit = def
	}
}
