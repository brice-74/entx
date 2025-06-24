package search

import (
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
)

type QueryBuildError = common.QueryBuildError
type ValidationError = common.ValidationError
type ExecError = common.ExecError

type Config = common.Config

type QueryPolicy = common.QueryPolicy
type QueryOp = common.QueryOp

const OpAggregate = common.OpAggregate
const OpAggregateOverall = common.OpAggregateOverall
const OpCountPaginate = common.OpCountPaginate
const OpRootQuery = common.OpRootQuery
const OpIncludeQuery = common.OpIncludeQuery

type AggregatesResponse = common.AggregatesResponse
type MetaResponse = common.MetaResponse
type MetaSearchResponse = common.MetaSearchResponse
type SearchResponse = common.SearchResponse
type SearchesResponse = common.SearchesResponse
type GroupResponse = common.GroupResponse
type GroupResponseSync = common.GroupResponseSync

type Select = dsl.Select
type Limit = dsl.Limit
type Pageable = dsl.Pageable

type Sorts = dsl.Sorts
type Sort = dsl.Sort

type Include = dsl.Include
type Includes = dsl.Includes

type Filter = dsl.Filter
type Filters = dsl.Filters

type Aggregate = dsl.Aggregate
type Aggregates = dsl.Aggregates
type OverallAggregate = dsl.OverallAggregate
type OverallAggregates = dsl.OverallAggregates
