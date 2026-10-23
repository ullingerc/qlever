// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/filterFacts/Canonicalizer.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// _____________________________________________________________________________
bool containsNow(const SparqlExpression& expr) {
  if (dynamic_cast<const sparqlExpression::NowDatetimeExpression*>(&expr)) {
    return true;
  }
  return ql::ranges::any_of(
      expr.children(), [](const auto& child) { return containsNow(*child); });
}

// Return true iff `BIND(expr AS ?v)` may be substituted, given the variables
// that are always bound before the `BIND`.
bool isSubstitutable(const SparqlExpression& expr,
                     const ad_utility::HashSet<Variable>& alwaysBound) {
  std::vector<const SparqlExpression*> exists;
  expr.getExistsExpressions(exists);
  return expr.isDeterministic() && !containsNow(expr) && exists.empty() &&
         ql::ranges::all_of(expr.containedVariables(),
                            [&alwaysBound](const Variable* var) {
                              return alwaysBound.contains(*var);
                            });
}

}  // namespace

// _____________________________________________________________________________
Definitions collectDefinitions(
    const std::vector<parsedQuery::GraphPatternOperation>& group) {
  Definitions defs;
  // ponytail: Only triples and `VALUES` count as always binding. Nested
  // groups, `UNION`s, subqueries etc. are ignored, which is conservative.
  ad_utility::HashSet<Variable> alwaysBound;
  for (const auto& op : group) {
    if (const auto* bgp = std::get_if<parsedQuery::BasicGraphPattern>(&op)) {
      bgp->collectAllContainedVariables(alwaysBound);
    } else if (const auto* values = std::get_if<parsedQuery::Values>(&op)) {
      const auto& [variables, rows] = values->_inlineValues;
      for (size_t i = 0; i < variables.size(); ++i) {
        std::vector<TripleComponent> column;
        for (const auto& row : rows) {
          column.push_back(row.at(i));
        }
        if (ql::ranges::any_of(column, &TripleComponent::isUndef)) {
          continue;
        }
        alwaysBound.insert(variables[i]);
        // If several `VALUES` clauses bind the same variable, the values of
        // any one of them are a sound over-approximation.
        defs.constants_.emplace(variables[i], std::move(column));
      }
    } else if (const auto* bind = std::get_if<parsedQuery::Bind>(&op)) {
      // The target is not always bound: the expression might be undefined.
      const auto* expr = bind->_expression.getPimpl();
      if (isSubstitutable(*expr, alwaysBound)) {
        defs.expressions_.emplace(bind->_target, expr);
      }
    }
  }
  return defs;
}

// _____________________________________________________________________________
std::optional<double> getNumericValue(const TripleComponent& tc) {
  if (tc.isInt()) {
    return static_cast<double>(tc.getInt());
  } else if (tc.isDouble()) {
    return tc.getDouble();
  } else if (tc.isId()) {
    const Id& id = tc.getId();
    if (id.getDatatype() == Datatype::Int) {
      return static_cast<double>(id.getInt());
    } else if (id.getDatatype() == Datatype::Double) {
      return id.getDouble();
    }
  }
  return std::nullopt;
}

namespace {

// _____________________________________________________________________________
TripleComponent toTripleComponent(const Id& id) {
  switch (id.getDatatype()) {
    case Datatype::Int:
      return TripleComponent{id.getInt()};
    case Datatype::Double:
      return TripleComponent{id.getDouble()};
    case Datatype::Bool:
      return TripleComponent{id.getBool()};
    default:
      return TripleComponent{id};
  }
}

// _____________________________________________________________________________
std::optional<double> fold(sparqlExpression::BinaryExpressionOperator op,
                           double a, double b) {
  using Op = sparqlExpression::BinaryExpressionOperator;
  switch (op) {
    case Op::Add:
      return a + b;
    case Op::Subtract:
      return a - b;
    case Op::Multiply:
      return a * b;
    case Op::Divide:
      // The result of a division by zero depends on a runtime parameter.
      return b == 0 ? std::nullopt : std::optional{a / b};
    default:
      return std::nullopt;
  }
}

// _____________________________________________________________________________
Comparison flip(Comparison op) {
  switch (op) {
    case Comparison::LT:
      return Comparison::GT;
    case Comparison::LE:
      return Comparison::GE;
    case Comparison::GT:
      return Comparison::LT;
    case Comparison::GE:
      return Comparison::LE;
    default:
      return op;
  }
}

// The recursive implementation of `canonicalize`.
class Canonicalizer {
  const Definitions& defs_;

 public:
  explicit Canonicalizer(const Definitions& defs) : defs_{defs} {}

  // Split `&&` and canonicalize each conjunct.
  void split(const SparqlExpression& expr, std::vector<Conjunct>& result) {
    if (sparqlExpression::getBinaryOperator(expr) ==
        sparqlExpression::BinaryExpressionOperator::And) {
      split(*expr.children()[0], result);
      split(*expr.children()[1], result);
    } else {
      result.push_back(toConjunct(expr));
    }
  }

 private:
  // If `expr` has a value from a statically known set of constants, return
  // these constants.
  std::optional<std::vector<TripleComponent>> toConstants(
      const SparqlExpression& expr) {
    using namespace sparqlExpression;
    if (auto var = expr.getVariableOrNullopt()) {
      if (auto it = defs_.constants_.find(var.value());
          it != defs_.constants_.end()) {
        return it->second;
      }
      if (auto it = defs_.expressions_.find(var.value());
          it != defs_.expressions_.end()) {
        return toConstants(*it->second);
      }
      return std::nullopt;
    }
    if (const auto* id = dynamic_cast<const IdExpression*>(&expr)) {
      return std::vector{toTripleComponent(id->value())};
    }
    if (const auto* lit = dynamic_cast<const StringLiteralExpression*>(&expr)) {
      return std::vector{TripleComponent{lit->value()}};
    }
    if (const auto* iri = dynamic_cast<const IriExpression*>(&expr)) {
      return std::vector{TripleComponent{iri->value()}};
    }
    if (auto op = getBinaryOperator(expr)) {
      return foldBinary(op.value(), expr);
    }
    return std::nullopt;
  }

  // Fold `a op b` for numeric constants `a` and `b`. At least one side must be
  // a single constant.
  std::optional<std::vector<TripleComponent>> foldBinary(
      sparqlExpression::BinaryExpressionOperator op,
      const SparqlExpression& expr) {
    auto left = toConstants(*expr.children()[0]);
    auto right = toConstants(*expr.children()[1]);
    if (!left || !right || (left->size() != 1 && right->size() != 1)) {
      return std::nullopt;
    }
    std::vector<TripleComponent> result;
    for (const auto& a : left.value()) {
      for (const auto& b : right.value()) {
        auto x = getNumericValue(a);
        auto y = getNumericValue(b);
        if (!x || !y) {
          return std::nullopt;
        }
        auto value = fold(op, x.value(), y.value());
        if (!value) {
          return std::nullopt;
        }
        result.emplace_back(value.value());
      }
    }
    return result;
  }

  // Bring `expr` into canonical form, substituting the definitions of
  // variables. Distances are returned in their original unit, together with
  // the factor which converts this unit to metres.
  std::pair<Term, double> toTerm(const SparqlExpression& expr) {
    using namespace sparqlExpression;
    if (auto var = expr.getVariableOrNullopt()) {
      if (auto it = defs_.expressions_.find(var.value());
          it != defs_.expressions_.end()) {
        return toTerm(*it->second);
      }
      return {var.value(), 1};
    }
    if (auto call = getGeoDistanceExpressionParameters(expr);
        call.has_value() && ad_utility::detail::isLengthUnit(call->unit_)) {
      double toMetres =
          ad_utility::detail::valueInUnitToKilometer(1, call->unit_) * 1000;
      return {Distance{resolve(call->left_), resolve(call->right_)}, toMetres};
    }
    if (auto var = getVariableFromLangExpression(&expr)) {
      return {Lang{var.value()}, 1};
    }
    return {Opaque{&expr}, 1};
  }

  // Replace a variable argument of a function by its value, if it has a
  // single one.
  TripleComponent resolve(const TripleComponent& tc) {
    if (tc.isVariable()) {
      if (auto it = defs_.constants_.find(tc.getVariable());
          it != defs_.constants_.end() && it->second.size() == 1) {
        return it->second.front();
      }
    }
    return tc;
  }

  // Build the `Constraint` `lhs op rhs` where `rhs` are constants.
  Conjunct makeConstraint(const SparqlExpression& lhs, Comparison op,
                          std::vector<TripleComponent> rhs) {
    auto [term, toMetres] = toTerm(lhs);
    if (toMetres != 1) {
      for (auto& c : rhs) {
        if (auto value = getNumericValue(c)) {
          c = TripleComponent{value.value() * toMetres};
        }
      }
    }
    return Constraint{std::move(term), op, std::move(rhs)};
  }

  // Bring a single conjunct into canonical form.
  Conjunct toConjunct(const SparqlExpression& expr) {
    using namespace sparqlExpression;
    if (auto op = getComparison(expr)) {
      const auto& left = *expr.children()[0];
      const auto& right = *expr.children()[1];
      if (auto constants = toConstants(right)) {
        return makeConstraint(left, op.value(), std::move(constants).value());
      }
      if (auto constants = toConstants(left)) {
        return makeConstraint(right, flip(op.value()),
                              std::move(constants).value());
      }
    } else if (dynamic_cast<const InExpression*>(&expr)) {
      // `lhs IN (c1, c2, ...)` is `lhs = c` for some `c`.
      auto children = expr.children();
      std::vector<TripleComponent> rhs;
      for (const auto& child : children.subspan(1)) {
        auto constants = toConstants(*child);
        if (!constants) {
          return Opaque{&expr};
        }
        ql::ranges::move(constants.value(), std::back_inserter(rhs));
      }
      return makeConstraint(*children[0], Comparison::EQ, std::move(rhs));
    } else if (getBinaryOperator(expr) == BinaryExpressionOperator::Or) {
      // `x = a || x = b` is `x = c` for some `c` in `{a, b}`.
      auto left = toConjunct(*expr.children()[0]);
      auto right = toConjunct(*expr.children()[1]);
      auto* l = std::get_if<Constraint>(&left);
      auto* r = std::get_if<Constraint>(&right);
      if (l && r && l->op_ == Comparison::EQ && r->op_ == Comparison::EQ &&
          l->lhs_ == r->lhs_) {
        ql::ranges::move(r->rhs_, std::back_inserter(l->rhs_));
        return left;
      }
    }
    return Opaque{&expr};
  }

  // If `expr` is a relational expression (`<`, `=`, etc.), return its
  // operator.
  static std::optional<Comparison> getComparison(const SparqlExpression& expr) {
    std::optional<Comparison> result;
    auto check = [&expr, &result](auto comp) {
      constexpr Comparison op = decltype(comp)::value;
      using T = sparqlExpression::relational::RelationalExpression<op>;
      if (dynamic_cast<const T*>(&expr) != nullptr) {
        result = op;
      }
    };
    using C = Comparison;
    check(std::integral_constant<C, C::LT>{});
    check(std::integral_constant<C, C::LE>{});
    check(std::integral_constant<C, C::EQ>{});
    check(std::integral_constant<C, C::NE>{});
    check(std::integral_constant<C, C::GT>{});
    check(std::integral_constant<C, C::GE>{});
    return result;
  }
};

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(const SparqlExpression& filter,
                                   const Definitions& defs) {
  std::vector<Conjunct> result;
  Canonicalizer{defs}.split(filter, result);
  return result;
}

}  // namespace filterFacts
