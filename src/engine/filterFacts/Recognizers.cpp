// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/filterFacts/Recognizers.h"

#include <cmath>
#include <limits>

namespace filterFacts {

using valueIdComparators::Comparison;

// _____________________________________________________________________________
std::optional<DistanceBound> recognizeDistanceBound(const Conjunct& conjunct) {
  // Only a `Constraint` bounds a distance, and only between two different
  // geometries, at least one of which is a variable.
  const auto* constraint = std::get_if<Constraint>(&conjunct);
  if (constraint == nullptr) {
    return std::nullopt;
  }
  const Distance& distance = constraint->lhs_;
  if (distance.a_ == distance.b_ ||
      (!distance.a_.isVariable() && !distance.b_.isVariable())) {
    return std::nullopt;
  }

  // Compute the maximum distance (for `<`, the largest `double` below the
  // number), and return `std::nullopt` if it is negative, infinite or NaN.
  double maxMeters = constraint->rhs_;
  if (constraint->op_ == Comparison::LT) {
    maxMeters =
        std::nextafter(maxMeters, -std::numeric_limits<double>::infinity());
  }
  if (!std::isfinite(maxMeters) || maxMeters < 0) {
    return std::nullopt;
  }
  return DistanceBound{distance.a_, distance.b_, maxMeters, constraint->exact_};
}

// _____________________________________________________________________________
std::optional<LangIn> recognizeLangIn(const Conjunct& conjunct) {
  const auto* langEquals = std::get_if<LangEquals>(&conjunct);
  if (langEquals == nullptr || langEquals->rhs_.empty()) {
    return std::nullopt;
  }
  ad_utility::HashSet<std::string> langs;
  for (const auto& constant : langEquals->rhs_) {
    if (!constant.isLiteral() || constant.getLiteral().hasLanguageTag() ||
        constant.getLiteral().hasDatatype()) {
      return std::nullopt;
    }
    langs.emplace(constant.getLiteral().getContent());
  }
  return LangIn{langEquals->var_, std::move(langs), langEquals->exact_};
}

}  // namespace filterFacts
