// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
#define QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_

#include <functional>

#include "parser/GraphPatternOperation.h"
#include "parser/VariableCounter.h"
#include "util/HashSet.h"

// This module contains helpers for analyzing the structure of graph patterns.

// _____________________________________________________________________________
namespace graphPatternAnalysis {

// An `OPTIONAL { ?input <predicate> ?output }` (or `OPTIONAL { ?output
// <predicate> ?input }`, then `inputIsSubject_` is `false`), where `?input` is
// bound before the `OPTIONAL` and `?output` is not. If `<predicate>` is
// functional in the direction from `?input` to `?output` (each value of
// `?input` has at most one value of `?output`) and `?input` is never `UNDEF`,
// the `OPTIONAL` only adds the column `?output`, without adding or removing
// rows.
struct SingleTripleOptional {
  Variable input_;
  std::string predicate_;
  Variable output_;
  bool inputIsSubject_;
};

// Return the `SingleTripleOptional` for `optional` if its body is a single
// triple with an IRI predicate and two distinct variables, exactly one of which
// is contained in `boundVariables`. Otherwise, return `std::nullopt`.
std::optional<SingleTripleOptional> getSingleTripleOptional(
    const parsedQuery::Optional& optional,
    const ad_utility::HashSet<Variable>& boundVariables);

// A top-level `OPTIONAL` of a `GraphPattern` that only adds a column if its
// predicate is functional (see `SingleTripleOptional`): its `?input` is bound
// by a triple of a `BasicGraphPattern` that precedes it, and its `?output`
// appears nowhere else in the `GraphPattern`.
struct ColumnAddingOptional {
  const parsedQuery::Optional* optional_;
  SingleTripleOptional triple_;
};
std::vector<ColumnAddingOptional> findColumnAddingOptionals(
    const parsedQuery::GraphPattern& gp);

// Check whether a given `GraphPatternOperation` is invariant with respect to
// the variable usage in the surrounding `GraphPattern`, meaning that removing
// it would not affect the result of the query. The variable usage is counted
// once up front (across the whole `GraphPattern`) into `variableCounts_`; a
// later `operator()(op)` query then asks whether `op`'s variables appear at
// most once in the pattern (i.e. only inside `op` itself).
//
// For example: a `BIND(... AS ?y)` is invariant when `?y` is not referenced
// anywhere else in the pattern (the `BIND` only adds its own column without
// adding or removing rows). A single-row `VALUES` clause is invariant when
// none of its variables are referenced elsewhere. An `OPTIONAL` is invariant if
// it is a `ColumnAddingOptional` and `isFunctional` returns `true` for it.
//
// This is currently used for the `MaterializedViewsManager`'s
// `QueryPatternCache`.
//
// NOTE: This does not guarantee completeness, so it might return `false` even
// though we could be invariant to a `GraphPatternOperation`.
struct BasicGraphPatternsInvariantTo {
  using IsFunctional = std::function<bool(const SingleTripleOptional&)>;

  parsedQuery::VariableCounter variableCounts_;
  // The `OPTIONAL`s of the `GraphPattern` that are invariant. They are
  // identified by their address, so the `GraphPattern` must not be moved or
  // modified before they have been checked.
  ad_utility::HashSet<const parsedQuery::Optional*> invariantOptionals_;

  // Initialize with a `GraphPattern`. Without `isFunctional`, no `OPTIONAL` is
  // invariant.
  explicit BasicGraphPatternsInvariantTo(const parsedQuery::GraphPattern& gp,
                                         const IsFunctional& isFunctional = {});

  explicit BasicGraphPatternsInvariantTo(parsedQuery::VariableCounter vc)
      : variableCounts_{std::move(vc)} {}

  bool operator()(const parsedQuery::Bind& bind) const;
  bool operator()(const parsedQuery::Values& values) const;
  bool operator()(const parsedQuery::Optional& optional) const;

  template <typename T>
  bool operator()(const T&) const {
    // The presence of any of these operations might remove or duplicate rows.
    namespace pq = parsedQuery;
    static_assert(
        ad_utility::SimilarToAny<
            T, pq::Union, pq::Subquery, pq::TransPath, pq::BasicGraphPattern,
            pq::Service, pq::PathQuery, pq::SpatialQuery, pq::TextSearchQuery,
            pq::Minus, pq::GroupGraphPattern, pq::Describe, pq::Load,
            pq::NamedCachedResult, pq::MaterializedViewQuery,
            pq::ExternalValuesQuery>);
    return false;
  }

 private:
  // Check that the given variable is counted at most once by `variableCounts_`.
  bool variableAppearsAtMostOnce(const Variable& var) const;
};

}  // namespace graphPatternAnalysis

#endif  // QLEVER_SRC_PARSER_GRAPHPATTERNANALYSIS_H_
