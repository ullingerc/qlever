// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
#define QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H

#include <absl/functional/function_ref.h>

#include <memory>

#include "parser/data/SparqlFilter.h"
#include "rdfTypes/Variable.h"

class QueryExecutionContext;
class SpatialJoin;
namespace filterFacts {
struct DistanceBound;
}

// This module contains utilities for query rewriting, e.g. optimizing cartesian
// product and filter by replacing it with an appropriate special join.

// Try to rewrite `filter` into an equivalent `SpatialJoin` operation, if
// `filter` is a suitable GeoSPARQL filter with a variable on at least one side.
// Returns `nullptr` if `filter` is not such a filter. A side of the filter that
// was a fixed value is bound to a single-row `VALUES` child using an internal
// variable from `generateUniqueVarName`. If `allowDistance` is false, filters
// on `geof:distance` are not rewritten (they are handled via
// `rewriteDistanceBoundToSpatialJoin` instead).
std::shared_ptr<SpatialJoin> rewriteFilterToSpatialJoin(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName,
    bool allowDistance = true);

// Create the `SpatialJoin` for a `DistanceBound` fact, see
// `engine/filterFacts/Recognizers.h`. Like `rewriteFilterToSpatialJoin`, a
// fixed geometry is bound to a single-row `VALUES` child.
std::shared_ptr<SpatialJoin> rewriteDistanceBoundToSpatialJoin(
    const filterFacts::DistanceBound& bound, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName);

// A FILTER that is equivalent to the `DistanceBound` fact, namely
// `geof:metricDistance(a, b) <= maxMeters`.
SparqlFilter distanceBoundToFilter(const filterFacts::DistanceBound& bound);

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
