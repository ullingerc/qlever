// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/Values.h"
#include "engine/filterFacts/Recognizers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"

namespace {

// A `TripleComponent` geo function operand resolved to a `Variable` (for
// `SpatialJoinConfiguration`) together with, for a fixed value, the one-row
// `VALUES` tree that binds it to a fresh internal variable.
struct ResolvedGeoOperand {
  Variable variable_;
  std::optional<std::shared_ptr<QueryExecutionTree>> child_ = std::nullopt;
};

// _____________________________________________________________________________
ResolvedGeoOperand resolveGeoOperand(
    const TripleComponent& operand, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  if (operand.isVariable()) {
    return {operand.getVariable()};
  }
  Variable var = generateUniqueVarName();
  auto tree = makeValuesForSingleValue(qec, var, operand);
  return {std::move(var), std::move(tree)};
}

}  // namespace

// Try the three supported filter patterns in turn and directly build the
// resulting `LibSpatialJoinConfig` together with the joined variables. Kept
// as a separate function (as opposed to three loose `optional`s in the
// caller that have to be kept in sync manually) so that the join type,
// maximum distance, and DE-9IM filter pattern -- which depend on each other
// -- can only leave this function bundled together in a single consistent
// `LibSpatialJoinConfig`.
static std::optional<
    std::pair<LibSpatialJoinConfig, sparqlExpression::GeoFunctionCall>>
getSpatialJoinConfigForFilter(
    const sparqlExpression::SparqlExpression& filterBody, bool allowDistance) {
  // The filter body directly is an optimizable `geof:sf...` function.
  if (auto call = getGeoFunctionExpressionParameters(filterBody)) {
    return std::pair{LibSpatialJoinConfig{call.value().function_},
                     std::move(call).value()};
  }
  // The filter body is a `geof:relate` call with an explicit DE-9IM filter
  // pattern.
  if (auto call = getDe9imRelationExpressionParameters(filterBody)) {
    LibSpatialJoinConfig config{call.value().function_, std::nullopt,
                                call.value().pattern_};
    return std::pair{std::move(config), std::move(call).value()};
  }
  // The filter body is a maximum distance spatial search (direct body of
  // filter is a comparison).
  if (auto call =
          allowDistance ? getGeoDistanceFilter(filterBody) : std::nullopt) {
    LibSpatialJoinConfig config{call.value().first.function_,
                                call.value().second, std::nullopt};
    return std::pair{std::move(config), std::move(call).value().first};
  }
  return std::nullopt;
}

// Create a `SpatialJoin` with the given `config` between the geo function
// operands `left` and `right`, see `rewriteFilterToSpatialJoin`.
static std::shared_ptr<SpatialJoin> makeSpatialJoin(
    LibSpatialJoinConfig config, const TripleComponent& left,
    const TripleComponent& right, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  // If neither side is a variable, rewriting is not possible.
  bool leftIsVar = left.isVariable();
  bool rightIsVar = right.isVariable();
  if (!leftIsVar && !rightIsVar) {
    return nullptr;
  }

  if (leftIsVar && rightIsVar && left.getVariable() == right.getVariable()) {
    // TODO<ullingerc> As soon as we have a baseline implementation of
    // `WktGeometricRelation`, replace this `throw` by `return nullptr;`.
    throw std::runtime_error(absl::StrCat(
        "Unsupported GeoSPARQL filter: Variable ", left.getVariable().name(),
        " on both sides. Is this what you intended?"));
  }

  auto l = resolveGeoOperand(left, qec, generateUniqueVarName);
  auto r = resolveGeoOperand(right, qec, generateUniqueVarName);
  return qec->makeShared<SpatialJoin>(
      qec,
      SpatialJoinConfiguration{
          std::move(config), std::move(l.variable_), std::move(r.variable_),
          std::nullopt, PayloadVariables::all(),
          SpatialJoinAlgorithm::LIBSPATIALJOIN, std::nullopt},
      std::move(l.child_), std::move(r.child_), true);
}

// _____________________________________________________________________________
std::shared_ptr<SpatialJoin> rewriteFilterToSpatialJoin(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName, bool allowDistance) {
  const auto& filterBody = *filter.expression_.getPimpl();

  // Currently, we can only optimize GeoSPARQL filters.
  auto configAndCall = getSpatialJoinConfigForFilter(filterBody, allowDistance);
  if (!configAndCall.has_value()) {
    return nullptr;
  }
  auto& [config, call] = configAndCall.value();
  return makeSpatialJoin(std::move(config), call.left_, call.right_, qec,
                         generateUniqueVarName);
}

// _____________________________________________________________________________
std::shared_ptr<SpatialJoin> rewriteDistanceBoundToSpatialJoin(
    const filterFacts::DistanceBound& bound, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  return makeSpatialJoin(LibSpatialJoinConfig{SpatialJoinType::WITHIN_DIST,
                                              bound.maxMeters_, std::nullopt},
                         bound.a_, bound.b_, qec, generateUniqueVarName);
}

// _____________________________________________________________________________
SparqlFilter distanceBoundToFilter(const filterFacts::DistanceBound& bound) {
  using namespace sparqlExpression;
  // The operands of a `DistanceBound` are variables or fixed geometries, see
  // `filterFacts::Canonicalizer`.
  auto toExpression = [](const TripleComponent& tc) -> SparqlExpression::Ptr {
    if (tc.isVariable()) {
      return std::make_unique<VariableExpression>(tc.getVariable());
    } else if (tc.isLiteral()) {
      return std::make_unique<StringLiteralExpression>(tc.getLiteral());
    } else if (tc.isId()) {
      return std::make_unique<IdExpression>(tc.getId());
    }
    const auto* point = std::get_if<GeoPoint>(&tc.getVariant());
    AD_CORRECTNESS_CHECK(point != nullptr);
    return std::make_unique<IdExpression>(ValueId::makeFromGeoPoint(*point));
  };
  auto expr =
      std::make_unique<LessEqualExpression>(LessEqualExpression::Children{
          makeMetricDistExpression(toExpression(bound.a_),
                                   toExpression(bound.b_)),
          std::make_unique<IdExpression>(
              Id::makeFromDouble(bound.maxMeters_))});
  auto descriptor =
      absl::StrCat("geof:metricDistance(", bound.a_.toString(), ", ",
                   bound.b_.toString(), ") <= ", bound.maxMeters_);
  return SparqlFilter{
      SparqlExpressionPimpl{std::move(expr), std::move(descriptor)}};
}
