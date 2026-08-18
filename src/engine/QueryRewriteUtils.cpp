// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

#include "engine/QueryExecutionTree.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"

using sparqlExpression::GeoOperand;

namespace {

// A `GeoOperand` resolved to a `Variable` (for `SpatialJoinConfiguration`)
// together with, for a fixed value, the one-row `VALUES` tree that binds it
// to a fresh internal variable.
struct ResolvedGeoOperand {
  Variable variable_;
  std::optional<std::shared_ptr<QueryExecutionTree>> child_ = std::nullopt;
};

// _____________________________________________________________________________
ResolvedGeoOperand resolveGeoOperand(
    const GeoOperand& operand, QueryExecutionContext* qec,
    const std::function<Variable()>& generateUniqueVarName) {
  if (const auto* var = std::get_if<Variable>(&operand)) {
    return {*var};
  }
  Variable var = generateUniqueVarName();
  auto tree = ad_utility::makeExecutionTree<Values>(
      qec,
      parsedQuery::SparqlValues{{var}, {{std::get<TripleComponent>(operand)}}});
  return {std::move(var), std::move(tree)};
}

}  // namespace

// Result of `getSpatialJoinConfigForFilter`: the `LibSpatialJoinConfig`, the
// joined-on operands, and -- only for a distance filter whose value came from
// a `BIND` (see `GeoDistanceFilterResult::distanceVariable_`) -- the variable
// and unit the distance should be exported to.
struct SpatialJoinConfigForFilter {
  LibSpatialJoinConfig config_;
  sparqlExpression::GeoFunctionCall call_;
  std::optional<Variable> distanceVariable_ = std::nullopt;
  std::optional<UnitOfMeasurement> distanceUnit_ = std::nullopt;
};

// Try the three supported filter patterns in turn and directly build the
// resulting `LibSpatialJoinConfig` together with the joined variables. Kept
// as a separate function (as opposed to three loose `optional`s in the
// caller that have to be kept in sync manually) so that the join type,
// maximum distance, and DE-9IM filter pattern -- which depend on each other
// -- can only leave this function bundled together in a single consistent
// `LibSpatialJoinConfig`.
static std::optional<SpatialJoinConfigForFilter> getSpatialJoinConfigForFilter(
    const sparqlExpression::SparqlExpression& filterBody,
    const ad_utility::HashMap<Variable, sparqlExpression::GeoDistanceCall>&
        boundDistanceVars) {
  // The filter body directly is an optimizable `geof:sf...` function.
  if (auto call = getGeoFunctionExpressionParameters(filterBody)) {
    LibSpatialJoinConfig config{call.value().function_};
    return SpatialJoinConfigForFilter{std::move(config),
                                      std::move(call).value()};
  }
  // The filter body is a `geof:relate` call with an explicit DE-9IM filter
  // pattern.
  if (auto call = getDe9imRelationExpressionParameters(filterBody)) {
    LibSpatialJoinConfig config{call.value().function_, std::nullopt,
                                call.value().pattern_};
    return SpatialJoinConfigForFilter{std::move(config),
                                      std::move(call).value()};
  }
  // The filter body is a maximum distance spatial search (direct body of
  // filter is a comparison, or a variable `BIND`ed to one).
  if (auto call = getGeoDistanceFilter(filterBody, boundDistanceVars)) {
    LibSpatialJoinConfig config{call.value().call_.function_,
                                call.value().maxDistMeters_, std::nullopt};
    return SpatialJoinConfigForFilter{
        std::move(config), std::move(call.value().call_),
        std::move(call.value().distanceVariable_), call.value().call_.unit_};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<SpatialJoinRewriteResult> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    const std::function<Variable()>& generateUniqueVarName,
    const ad_utility::HashMap<Variable, sparqlExpression::GeoDistanceCall>&
        boundDistanceVars) {
  const auto& filterBody = *filter.expression_.getPimpl();

  // Currently, we can only optimize GeoSPARQL filters.
  auto configForFilter =
      getSpatialJoinConfigForFilter(filterBody, boundDistanceVars);
  if (!configForFilter.has_value()) {
    return std::nullopt;
  }
  auto& [config, call, distanceVariable, distanceUnit] =
      configForFilter.value();

  // If neither side is a variable, there is nothing to join on. Leave this
  // (rare, degenerate) case to ordinary `FILTER` evaluation.
  bool leftIsVar = std::holds_alternative<Variable>(call.left_);
  bool rightIsVar = std::holds_alternative<Variable>(call.right_);
  if (!leftIsVar && !rightIsVar) {
    return std::nullopt;
  }

  if (leftIsVar && rightIsVar &&
      std::get<Variable>(call.left_) == std::get<Variable>(call.right_)) {
    // TODO<ullingerc> As soon as we have a baseline implementation of
    // `WktGeometricRelation`, replace this `throw` by `return std::nullopt;`.
    throw std::runtime_error(
        absl::StrCat("Unsupported GeoSPARQL filter: Variable ",
                     std::get<Variable>(call.left_).name(),
                     " on both sides. Is this what you intended?"));
  }

  auto joinType = call.function_;
  auto left = resolveGeoOperand(call.left_, qec, generateUniqueVarName);
  auto right = resolveGeoOperand(call.right_, qec, generateUniqueVarName);
  return SpatialJoinRewriteResult{
      SpatialJoinConfiguration{
          std::move(config), std::move(left.variable_),
          std::move(right.variable_), std::move(distanceVariable),
          std::move(distanceUnit), PayloadVariables::all(),
          SpatialJoinAlgorithm::LIBSPATIALJOIN, joinType, std::nullopt},
      std::move(left.child_), std::move(right.child_)};
}
