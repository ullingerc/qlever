// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
#define QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H

#include <functional>
#include <memory>

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "parser/data/SparqlFilter.h"
#include "util/HashMap.h"

class QueryExecutionContext;
class QueryExecutionTree;

// This module contains utilities for query rewriting, e.g. optimizing cartesian
// product and filter by replacing it with an appropriate special join.

// Result of `rewriteFilterToSpatialJoinConfig`. `childLeft_`/`childRight_` are
// set for a side of `config_` that was a fixed value (not a variable) in the
// original filter: an already-built one-row `VALUES` tree that binds the
// fresh internal variable `config_.left_`/`right_` to that value. A
// `std::nullopt` child means that side is an ordinary variable, which the
// query planner still has to connect via the join graph, exactly as before.
struct SpatialJoinRewriteResult {
  SpatialJoinConfiguration config_;
  std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_;
  std::optional<std::shared_ptr<QueryExecutionTree>> childRight_;
};

// Generate a spatial join configuration for a given filter, if this filter is
// suitable for such an optimization. `generateUniqueVarName` is used to
// obtain a fresh internal variable for each side of the filter that is a
// fixed value rather than a variable. `boundDistanceVars` maps the target
// variable of a `BIND(geof:distance(...) AS ?dist)` (or `metricDistance`) to
// its parsed call, so that `FILTER(?dist <= constant)` can be recognized as
// the same pattern as a filter that spells out the distance call directly;
// see `QueryPlanner::collectGeoDistanceBinds`.
std::optional<SpatialJoinRewriteResult> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    const std::function<Variable()>& generateUniqueVarName,
    const ad_utility::HashMap<Variable, sparqlExpression::GeoDistanceCall>&
        boundDistanceVars = {});

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
