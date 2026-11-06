// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/filterFacts/Canonicalizer.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "parser/ParsedQuery.h"
#include "parser/VariableCounter.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// Return true iff `expression` contains `NOW()`.
bool containsNow(const SparqlExpression& expression) {
  if (dynamic_cast<const sparqlExpression::NowDatetimeExpression*>(
          &expression)) {
    return true;
  }
  return ql::ranges::any_of(expression.children(), [](const auto& child) {
    return containsNow(*child);
  });
}

// Return true iff the target of `BIND(expression AS ?v)` is equivalent to
// `expression`, given the variables that are always bound before the `BIND`.
bool isSubstitutable(const SparqlExpression& expression,
                     const ad_utility::HashSet<Variable>& alwaysBound) {
  std::vector<const SparqlExpression*> exists;
  expression.getExistsExpressions(exists);
  return expression.isDeterministic() && !containsNow(expression) &&
         exists.empty() &&
         ql::ranges::all_of(expression.containedVariables(),
                            [&alwaysBound](const Variable* variable) {
                              return alwaysBound.contains(*variable);
                            });
}

}  // namespace

// _____________________________________________________________________________
Definitions collectDefinitions(
    const std::vector<parsedQuery::GraphPatternOperation>& group) {
  Definitions defs;
  // The variables that are always bound by the patterns so far. Only triples
  // and `VALUES` count (ignoring nested groups etc. is conservative).
  ad_utility::HashSet<Variable> alwaysBound;
  for (const auto& operation : group) {
    // Remove the `BIND`s whose target `operation` may bind again, that is, any
    // variable that `operation` contains (where the expression is undefined,
    // the target is then bound freely, so it is not equivalent to the
    // expression).
    //
    // NOTE: A later `BIND` cannot bind the target again (the target of a
    // `BIND` must not be in scope before it), so its expression may use the
    // target.
    if (!std::holds_alternative<parsedQuery::Bind>(operation)) {
      parsedQuery::VariableCounter variables;
      variables(operation);
      absl::erase_if(defs.expressions_, [&variables](const auto& definition) {
        return variables.counts().contains(definition.first);
      });
    }

    // Add the definitions of `operation`.
    if (const auto* bgp =
            std::get_if<parsedQuery::BasicGraphPattern>(&operation)) {
      bgp->collectAllContainedVariables(alwaysBound);
    } else if (const auto* values =
                   std::get_if<parsedQuery::Values>(&operation)) {
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
        // If several `VALUES` clauses bind the same variable, each of them
        // contains all its values.
        defs.constants_.emplace(variables[i], std::move(column));
      }
    } else if (const auto* bind = std::get_if<parsedQuery::Bind>(&operation)) {
      // The target is not always bound (the expression may be undefined).
      const auto* expression = bind->_expression.getPimpl();
      if (isSubstitutable(*expression, alwaysBound)) {
        defs.expressions_.emplace(bind->_target, expression);
      }
    }
  }
  return defs;
}

namespace {

// Return the operator if `expression` is a comparison with `<` or `<=`.
std::optional<Comparison> getComparison(const SparqlExpression& expression) {
  if (dynamic_cast<const sparqlExpression::LessThanExpression*>(&expression)) {
    return Comparison::LT;
  } else if (dynamic_cast<const sparqlExpression::LessEqualExpression*>(
                 &expression)) {
    return Comparison::LE;
  }
  return std::nullopt;
}

// Return `expression`, or the expression of the `BIND` in `defs` if it is a
// variable with such a definition.
const SparqlExpression& resolve(const SparqlExpression& expression,
                                const Definitions& defs) {
  if (auto variable = expression.getVariableOrNullopt()) {
    if (auto it = defs.expressions_.find(variable.value());
        it != defs.expressions_.end()) {
      return resolve(*it->second, defs);
    }
  }
  return expression;
}

// Return the value of `expression` if it is a number (`xsd:int` or
// `xsd:double`) or a variable with a single number in `defs`, and
// `std::nullopt` otherwise (e.g. for the string "500").
std::optional<double> getNumber(const SparqlExpression& expression,
                                const Definitions& defs) {
  const auto& resolved = resolve(expression, defs);

  // A variable with a single number from a `VALUES` clause.
  if (auto variable = resolved.getVariableOrNullopt()) {
    auto it = defs.constants_.find(variable.value());
    if (it == defs.constants_.end() || it->second.size() != 1) {
      return std::nullopt;
    }
    const auto& constant = it->second.front();
    if (constant.isInt()) {
      return static_cast<double>(constant.getInt());
    } else if (constant.isDouble()) {
      return constant.getDouble();
    }
    return std::nullopt;
  }

  // A number.
  const auto* id =
      dynamic_cast<const sparqlExpression::IdExpression*>(&resolved);
  if (id == nullptr) {
    return std::nullopt;
  }
  if (id->value().getDatatype() == Datatype::Int) {
    return static_cast<double>(id->value().getInt());
  } else if (id->value().getDatatype() == Datatype::Double) {
    return id->value().getDouble();
  }
  return std::nullopt;
}

// Return the possible values of `expression` if it is a literal or a variable
// with values in `defs` (and `std::nullopt` otherwise).
std::optional<std::vector<TripleComponent>> getConstants(
    const SparqlExpression& expression, const Definitions& defs) {
  const auto& resolved = resolve(expression, defs);
  if (auto variable = resolved.getVariableOrNullopt()) {
    auto it = defs.constants_.find(variable.value());
    if (it == defs.constants_.end() || it->second.empty()) {
      return std::nullopt;
    }
    return it->second;
  }
  if (const auto* literal =
          dynamic_cast<const sparqlExpression::StringLiteralExpression*>(
              &resolved)) {
    return std::vector{TripleComponent{literal->value()}};
  }
  return std::nullopt;
}

// Return the canonical form of `LANG(?x) = c`, `c = LANG(?x)`,
// `LANG(?x) IN (c1, c2, ...)` and `||` of such comparisons for the same `?x`,
// and `std::nullopt` for every other expression.
std::optional<LangEquals> toLangEquals(const SparqlExpression& expression,
                                       const Definitions& defs) {
  using namespace sparqlExpression;
  auto children = expression.children();

  // Merge the constants of the two sides of `||` (`LANG(?x) = "en" ||
  // LANG(?x) = "de"` is `LANG(?x) IN ("en", "de")`).
  if (isOrExpression(expression)) {
    auto left = toLangEquals(*children[0], defs);
    auto right = toLangEquals(*children[1], defs);
    if (!left.has_value() || !right.has_value() || left->var_ != right->var_) {
      return std::nullopt;
    }
    ql::ranges::move(right->rhs_, std::back_inserter(left->rhs_));
    left->exact_ = left->exact_ && right->exact_;
    return left;
  }

  // Find `LANG(?x)` and the operands that it is compared with.
  auto getLangVariable = [&defs](const SparqlExpression& operand) {
    return getVariableFromLangExpression(&resolve(operand, defs));
  };
  bool isEqual = dynamic_cast<const EqualExpression*>(&expression);
  if (!isEqual && !dynamic_cast<const InExpression*>(&expression)) {
    return std::nullopt;
  }
  auto variable = getLangVariable(*children[0]);
  auto constants = children.subspan(1);
  if (isEqual && !variable.has_value()) {
    variable = getLangVariable(*children[1]);
    constants = children.first(1);
  }
  if (!variable.has_value()) {
    return std::nullopt;
  }

  // Collect the values of these operands. The comparison is not exact if an
  // operand has different values in different rows.
  LangEquals result{variable.value(), {}};
  for (const auto& constant : constants) {
    auto values = getConstants(*constant, defs);
    if (!values.has_value()) {
      return std::nullopt;
    }
    result.exact_ =
        result.exact_ &&
        ql::ranges::all_of(values.value(), [&values](const auto& value) {
          return value == values->front();
        });
    ql::ranges::move(values.value(), std::back_inserter(result.rhs_));
  }
  return result;
}

// Return the canonical form of a single conjunct: `distance < number` and
// `distance <= number` become a `Constraint`, a comparison of `LANG(?x)` with
// constants becomes a `LangEquals`, everything else is `Opaque`.
Conjunct toConjunct(const SparqlExpression& expression,
                    const Definitions& defs) {
  if (auto langEquals = toLangEquals(expression, defs)) {
    return std::move(langEquals).value();
  }
  auto op = getComparison(expression);
  if (!op.has_value()) {
    return Opaque{&expression};
  }
  auto call = sparqlExpression::getGeoDistanceExpressionParameters(
      resolve(*expression.children()[0], defs));
  auto number = getNumber(*expression.children()[1], defs);
  if (!call.has_value() || !ad_utility::detail::isLengthUnit(call->unit_) ||
      !number.has_value()) {
    return Opaque{&expression};
  }

  // Convert the number from the unit of the distance to meters.
  double meters =
      ad_utility::detail::valueInUnitToKilometer(number.value(), call->unit_) *
      1000;
  return Constraint{Distance{call->left_, call->right_}, op.value(), meters};
}

// Split `expression` at `&&` and append the canonical form of each conjunct to
// `result`.
void split(const SparqlExpression& expression, const Definitions& defs,
           std::vector<Conjunct>& result) {
  if (sparqlExpression::isAndExpression(expression)) {
    for (const auto& child : expression.children()) {
      split(*child, defs, result);
    }
  } else {
    result.push_back(toConjunct(expression, defs));
  }
}

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(const SparqlExpression& filter,
                                   const Definitions& defs) {
  std::vector<Conjunct> result;
  split(filter, defs, result);
  return result;
}

}  // namespace filterFacts
