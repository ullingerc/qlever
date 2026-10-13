// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "parser/GraphPatternAnalysis.h"

namespace graphPatternAnalysis {

// _____________________________________________________________________________
std::optional<SingleTripleOptional> getSingleTripleOptional(
    const parsedQuery::Optional& optional,
    const ad_utility::HashSet<Variable>& boundVariables) {
  const auto& child = optional._child;
  if (!child._filters.empty() || child._graphPatterns.size() != 1) {
    return std::nullopt;
  }
  const auto* bgp =
      std::get_if<parsedQuery::BasicGraphPattern>(&child._graphPatterns[0]);
  if (bgp == nullptr || bgp->_triples.size() != 1) {
    return std::nullopt;
  }
  const auto& triple = bgp->_triples[0];
  auto predicate = triple.getSimplePredicate();
  if (!predicate.has_value() || !triple.s_.isVariable() ||
      !triple.o_.isVariable()) {
    return std::nullopt;
  }
  const auto& s = triple.s_.getVariable();
  const auto& o = triple.o_.getVariable();
  bool sBound = boundVariables.contains(s);
  bool oBound = boundVariables.contains(o);
  if (s == o || sBound == oBound) {
    return std::nullopt;
  }
  return sBound
             ? SingleTripleOptional{s, std::string{predicate.value()}, o, true}
             : SingleTripleOptional{o, std::string{predicate.value()}, s,
                                    false};
}

// _____________________________________________________________________________
std::vector<ColumnAddingOptional> findColumnAddingOptionals(
    const parsedQuery::GraphPattern& gp) {
  parsedQuery::VariableCounter counter;
  counter(gp);
  std::vector<ColumnAddingOptional> result;
  // The variables bound (and never `UNDEF`) by the triples seen so far.
  ad_utility::HashSet<Variable> boundVariables;
  for (const auto& operation : gp._graphPatterns) {
    if (const auto* bgp =
            std::get_if<parsedQuery::BasicGraphPattern>(&operation)) {
      for (const auto& triple : bgp->_triples) {
        for (const auto* side : {&triple.s_, &triple.o_}) {
          if (side->isVariable()) {
            boundVariables.insert(side->getVariable());
          }
        }
      }
    } else if (const auto* optional =
                   std::get_if<parsedQuery::Optional>(&operation)) {
      auto triple = getSingleTripleOptional(*optional, boundVariables);
      if (triple.has_value() &&
          counter.counts().at(triple.value().output_) == 1) {
        result.push_back({optional, std::move(triple.value())});
      }
    }
  }
  return result;
}

// _____________________________________________________________________________
BasicGraphPatternsInvariantTo::BasicGraphPatternsInvariantTo(
    const parsedQuery::GraphPattern& gp, const IsFunctional& isFunctional) {
  variableCounts_(gp);
  if (!isFunctional) {
    return;
  }
  for (const auto& [optional, triple] : findColumnAddingOptionals(gp)) {
    if (isFunctional(triple)) {
      invariantOptionals_.insert(optional);
    }
  }
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Bind& bind) const {
  return variableAppearsAtMostOnce(bind._target);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Values& valuesClause) const {
  const auto& [variables, values] = valuesClause._inlineValues;
  return
      // There is exactly one row inside the `VALUES`.
      values.size() == 1 &&
      // Each `VALUES` variable appears at most once across the whole pattern.
      ql::ranges::all_of(variables, [this](const auto& var) {
        return variableAppearsAtMostOnce(var);
      });
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Optional& optional) const {
  return invariantOptionals_.contains(&optional);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::variableAppearsAtMostOnce(
    const Variable& var) const {
  const auto& counts = variableCounts_.counts();
  auto it = counts.find(var);
  return it == counts.end() || it->second <= 1;
}

}  // namespace graphPatternAnalysis
