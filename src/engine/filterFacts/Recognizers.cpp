// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/filterFacts/Recognizers.h"

#include <cmath>
#include <limits>

namespace filterFacts {

using valueIdComparators::Comparison;

// _____________________________________________________________________________
std::optional<DistanceBound> recognizeDistanceBound(const Conjunct& conjunct) {
  const auto* constraint = std::get_if<Constraint>(&conjunct);
  if (!constraint || (constraint->op_ != Comparison::LE &&
                      constraint->op_ != Comparison::LT)) {
    return std::nullopt;
  }
  const auto* distance = std::get_if<Distance>(&constraint->lhs_);
  if (!distance || distance->a_ == distance->b_ ||
      (!distance->a_.isVariable() && !distance->b_.isVariable())) {
    return std::nullopt;
  }
  // `distance <= c` can only hold for a numeric `c`.
  double maxMeters = -1;
  for (const auto& c : constraint->rhs_) {
    auto value = getNumericValue(c);
    if (!value || std::isnan(value.value())) {
      return std::nullopt;
    }
    maxMeters = std::max(maxMeters, value.value());
  }
  if (maxMeters < 0) {
    return std::nullopt;
  }
  if (constraint->op_ == Comparison::LT) {
    maxMeters =
        std::nextafter(maxMeters, -std::numeric_limits<double>::infinity());
  }
  return DistanceBound{distance->a_, distance->b_, maxMeters,
                       constraint->exact_};
}

// _____________________________________________________________________________
std::optional<LangIn> recognizeLangIn(const Conjunct& conjunct) {
  const auto* constraint = std::get_if<Constraint>(&conjunct);
  if (!constraint || constraint->op_ != Comparison::EQ) {
    return std::nullopt;
  }
  const auto* lang = std::get_if<Lang>(&constraint->lhs_);
  if (!lang || constraint->rhs_.empty()) {
    return std::nullopt;
  }
  // `LANG` returns a plain literal, so it can only be equal to one.
  ad_utility::HashSet<std::string> langs;
  for (const auto& c : constraint->rhs_) {
    if (!c.isLiteral() || c.getLiteral().hasLanguageTag() ||
        c.getLiteral().hasDatatype()) {
      return std::nullopt;
    }
    langs.emplace(c.getLiteral().getContent());
  }
  return LangIn{lang->var_, std::move(langs), constraint->exact_};
}

// _____________________________________________________________________________
std::optional<Fact> recognize(const Conjunct& conjunct) {
  if (auto fact = recognizeDistanceBound(conjunct)) {
    return fact.value();
  }
  if (auto fact = recognizeLangIn(conjunct)) {
    return fact.value();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::vector<FilterFacts> recognizeFacts(
    const parsedQuery::GraphPattern& group) {
  auto defs = collectDefinitions(group._graphPatterns);
  std::vector<FilterFacts> result;
  for (const auto& filter : group._filters) {
    // The FILTER is exact iff each of its conjuncts is an exact fact.
    FilterFacts& facts = result.emplace_back();
    facts.exact_ = true;
    for (const auto& conjunct :
         canonicalize(*filter.expression_.getPimpl(), defs)) {
      auto fact = recognize(conjunct);
      facts.exact_ &=
          fact.has_value() &&
          std::visit([](const auto& f) { return f.exact_; }, fact.value());
      if (fact.has_value()) {
        facts.facts_.push_back(std::move(fact).value());
      }
    }
  }
  return result;
}

}  // namespace filterFacts
