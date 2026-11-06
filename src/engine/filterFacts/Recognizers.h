// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H
#define QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "engine/filterFacts/Canonicalizer.h"
#include "parser/GraphPattern.h"
#include "util/HashSet.h"

// The last stage of the filter fact recognition (see
// `docs/experimental/filter-fact-recognition.md`): recognize typed facts in the
// canonical conjuncts of FILTERs. Each recognizer only has to be sound (if the
// FILTER holds, then the fact holds), it may over-approximate. If a fact is
// exact (the FILTER holds iff the fact holds), the FILTER can be replaced by
// the fact, otherwise it stays in the query plan.
namespace filterFacts {

// The distance between the geometries `a_` and `b_` (each a variable or a
// constant, at least one of them a variable) is at most `maxMeters_`.
struct DistanceBound {
  TripleComponent a_;
  TripleComponent b_;
  double maxMeters_;
  // True iff the conjunct holds exactly when the fact holds.
  bool exact_ = true;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(DistanceBound, a_, b_, maxMeters_,
                                        exact_)
};

// The language tag of the literal bound to `var_` is one of `langs_`.
struct LangIn {
  Variable var_;
  ad_utility::HashSet<std::string> langs_;
  // True iff the conjunct holds exactly when the fact holds.
  bool exact_ = true;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(LangIn, var_, langs_, exact_)
};

using Fact = std::variant<DistanceBound, LangIn>;

// The facts implied by the conjuncts of a single FILTER, in the order of
// `SparqlExpressionPimpl::splitConjunction`, with `std::nullopt` for a
// conjunct without a fact.
using FilterFacts = std::vector<std::optional<Fact>>;

// Return true iff `fact` exists and is exact (the conjunct holds iff the fact
// holds).
bool isExact(const std::optional<Fact>& fact);

// `distance(a, b) <= c` or `distance(a, b) < c` for numeric constants `c`. If
// there are several constants, the largest one is used. For `<`, the largest
// double below `c` is used (such that the fact remains exact).
std::optional<DistanceBound> recognizeDistanceBound(const Conjunct& conjunct);

// `LANG(?x) = "l"` for plain string literals `"l"`.
std::optional<LangIn> recognizeLangIn(const Conjunct& conjunct);

// Try all the recognizers above.
std::optional<Fact> recognize(const Conjunct& conjunct);

// Recognize the facts that are implied by each FILTER of `group` (in the same
// order as `group._filters`).
std::vector<FilterFacts> recognizeFacts(const parsedQuery::GraphPattern& group);

}  // namespace filterFacts

#endif  // QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H
