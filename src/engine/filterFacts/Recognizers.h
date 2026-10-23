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
// FILTER holds, then the fact holds), it may over-approximate, because the
// original FILTER always stays in the query plan.
namespace filterFacts {

// The distance between the geometries `a_` and `b_` (each a variable or a
// constant, at least one of them a variable) is at most `maxMeters_`.
struct DistanceBound {
  TripleComponent a_;
  TripleComponent b_;
  double maxMeters_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(DistanceBound, a_, b_, maxMeters_)
};

// The language tag of the literal bound to `var_` is one of `langs_`.
struct LangIn {
  Variable var_;
  ad_utility::HashSet<std::string> langs_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(LangIn, var_, langs_)
};

using Fact = std::variant<DistanceBound, LangIn>;

// `distance(a, b) <= c` or `distance(a, b) < c` for numeric constants `c`. If
// there are several constants, the largest one is used.
std::optional<DistanceBound> recognizeDistanceBound(const Conjunct& conjunct);

// `LANG(?x) = "l"` for plain string literals `"l"`.
std::optional<LangIn> recognizeLangIn(const Conjunct& conjunct);

// Try all the recognizers above.
std::optional<Fact> recognize(const Conjunct& conjunct);

// Recognize all facts that are implied by the FILTERs of `group`.
std::vector<Fact> recognizeFacts(const parsedQuery::GraphPattern& group);

}  // namespace filterFacts

#endif  // QLEVER_SRC_ENGINE_FILTERFACTS_RECOGNIZERS_H
