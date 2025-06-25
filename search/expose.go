package search

import (
	"github.com/brice-74/entx/search/common"
)

type QueryBuildError = common.QueryBuildError
type ValidationError = common.ValidationError
type ExecError = common.ExecError

type Config = common.Config

type QueryPolicy = common.QueryPolicy
type QueryOp = common.QueryOp

const OpAggregate = common.OpAggregate
const OpAggregateOverall = common.OpAggregateOverall
const OpRootQuery = common.OpRootQuery
const OpIncludeQuery = common.OpIncludeQuery
const OpLastIncludeQuery = common.OpLastIncludeQuery

type AggregatesResponse = common.AggregatesResponse
type MetaResponse = common.MetaResponse
type MetaSearchResponse = common.MetaSearchResponse
type SearchResponse = common.SearchResponse
type SearchesResponse = common.SearchesResponse
type GroupResponse = common.GroupResponse
type GroupResponseSync = common.GroupResponseSync
