// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_FILTERFACTS_CANONICALIZER_H
#define QLEVER_SRC_ENGINE_FILTERFACTS_CANONICALIZER_H

#include <optional>
#include <variant>
#include <vector>

#include "backports/three_way_comparison.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"
#include "parser/GraphPatternOperation.h"
#include "parser/TripleComponent.h"
#include "util/HashMap.h"

// The first two stages of the filter fact recognition (see
// `docs/experimental/filter-fact-recognition.md`): collect the definitions of
// the variables of a group graph pattern, and bring the FILTER expressions of
// the group into a canonical form using these definitions.
namespace filterFacts {

// What is statically known about the variables of a single group graph
// pattern. The definitions never cross the boundaries of the group.
struct Definitions {
  // The possible values of a variable, from an inline `VALUES` clause of the
  // group without `UNDEF` in the column of the variable. Every result row of
  // the group binds the variable to one of these values.
  ad_utility::HashMap<Variable, std::vector<TripleComponent>> constants_;
  // `BIND(e AS ?v)` clauses of the group that can be substituted: `e` is
  // deterministic, contains neither `NOW()` nor `EXISTS`, and all its input
  // variables are always bound by the patterns before the `BIND`. The pointers
  // point into the group graph pattern, which must outlive this struct.
  ad_utility::HashMap<Variable, const sparqlExpression::SparqlExpression*>
      expressions_;
};

// Collect the `Definitions` from the direct children of a group graph pattern.
Definitions collectDefinitions(
    const std::vector<parsedQuery::GraphPatternOperation>& group);

// The canonical form of an expression. Only the shapes required by the
// recognizers are modelled, everything else is `Opaque`.

// `geof:distance(a_, b_)` in metres. `a_` and `b_` are variables or constants.
struct Distance {
  TripleComponent a_;
  TripleComponent b_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Distance, a_, b_)
};

// `LANG(var_)`.
struct Lang {
  Variable var_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Lang, var_)
};

// Any other expression. It is kept unchanged, in particular nothing is
// substituted inside of it (e.g. inside of `BOUND(...)` or `EXISTS {...}`).
struct Opaque {
  const sparqlExpression::SparqlExpression* expr_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Opaque, expr_)
};

using Term = std::variant<Variable, Distance, Lang, Opaque>;

// `lhs_ op_ c` holds for at least one constant `c` from `rhs_`. Several
// constants come from `IN`, from `||` of equalities, or from a multi-row
// `VALUES` clause.
struct Constraint {
  Term lhs_;
  valueIdComparators::Comparison op_;
  std::vector<TripleComponent> rhs_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Constraint, lhs_, op_, rhs_)
};

// A single conjunct of a FILTER: if the FILTER holds, each of its conjuncts
// holds.
using Conjunct = std::variant<Constraint, Opaque>;

// Bring the expression of a FILTER into canonical form: split `&&` into
// conjuncts, substitute `defs`, fold constants, move the constant side of a
// comparison to the right, and normalize distances to metres.
std::vector<Conjunct> canonicalize(
    const sparqlExpression::SparqlExpression& filter, const Definitions& defs);

// Return the value of `tc` if it is a numeric constant (`xsd:int` or
// `xsd:double`), and `std::nullopt` otherwise (e.g. for the string "500").
std::optional<double> getNumericValue(const TripleComponent& tc);

}  // namespace filterFacts

#endif  // QLEVER_SRC_ENGINE_FILTERFACTS_CANONICALIZER_H
