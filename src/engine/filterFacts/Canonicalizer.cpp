// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/filterFacts/Canonicalizer.h"

#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "parser/ParsedQuery.h"
#include "parser/VariableCounter.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// Return true iff the target of `BIND(expression AS ?v)` is equivalent to
// `expression`, given the variables that are always bound before the `BIND`.
bool isSubstitutable(const SparqlExpression& expression,
                     const ad_utility::HashSet<Variable>& alwaysBound) {
  std::vector<const SparqlExpression*> exists;
  expression.getExistsExpressions(exists);
  return expression.isDeterministic() && exists.empty() &&
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
  // The variables that are always bound by the patterns so far. Only triples,
  // `VALUES` and `BIND`s of constants count (ignoring nested groups etc. is
  // conservative).
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
      // The target of a constant other than `UNDEF` is always bound to it,
      // like a `VALUES` clause with a single row. The target of any other
      // expression is not always bound (the expression may be undefined).
      auto constant = bind->_expression.getConstantValue();
      if (constant.has_value() &&
          !(constant->isId() && constant->getId().isUndefined())) {
        alwaysBound.insert(bind->_target);
        defs.constants_.emplace(bind->_target,
                                std::vector{std::move(constant).value()});
      } else if (const auto* expression = bind->_expression.getPimpl();
                 isSubstitutable(*expression, alwaysBound)) {
        defs.expressions_.emplace(bind->_target, expression);
      }
    }
  }
  return defs;
}

namespace {

// Return the operator if `expression` is a comparison with `<` or `<=`, and
// `false`. For `>` and `>=`, return the operator of the equivalent comparison
// with swapped operands (`b > a` is `a < b`), and `true`.
std::optional<std::pair<Comparison, bool>> getLessComparison(
    const SparqlExpression& expression) {
  using enum Comparison;
  auto op = sparqlExpression::getComparison(expression);
  if (op == LT || op == LE) {
    return std::pair{op.value(), false};
  } else if (op == GT || op == GE) {
    return std::pair{op == GT ? LT : LE, true};
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

// Return `a op b`, and `std::nullopt` for a division by zero (whose result
// depends on a runtime parameter).
std::optional<double> fold(sparqlExpression::ArithmeticOperator op, double a,
                           double b) {
  using enum sparqlExpression::ArithmeticOperator;
  switch (op) {
    case Add:
      return a + b;
    case Subtract:
      return a - b;
    case Multiply:
      return a * b;
    case Divide:
      return b == 0 ? std::nullopt : std::optional{a / b};
  }
  AD_FAIL();
}

// Return the possible values of `expression` if it is a number, a variable
// with only numbers in `defs`, or `+`, `-`, `*` or `/` of such expressions
// (and `std::nullopt` otherwise).
std::optional<std::vector<double>> getNumbers(
    const SparqlExpression& expression, const Definitions& defs) {
  const auto& resolved = resolve(expression, defs);

  // A variable with the numbers of a `VALUES` clause or a `BIND`.
  if (auto variable = resolved.getVariableOrNullopt()) {
    auto it = defs.constants_.find(variable.value());
    if (it == defs.constants_.end() || it->second.empty()) {
      return std::nullopt;
    }
    std::vector<double> numbers;
    for (const auto& constant : it->second) {
      auto number = constant.getNumber();
      if (!number.has_value()) {
        return std::nullopt;
      }
      numbers.push_back(number.value());
    }
    return numbers;
  }

  // The result of `+`, `-`, `*` or `/` for each pair of values of the two
  // operands, one of which must have a single value (to keep the number of
  // values small).
  if (auto op = sparqlExpression::getArithmeticOperator(resolved)) {
    auto left = getNumbers(*resolved.children()[0], defs);
    auto right = getNumbers(*resolved.children()[1], defs);
    if (!left.has_value() || !right.has_value() ||
        (left->size() != 1 && right->size() != 1)) {
      return std::nullopt;
    }
    std::vector<double> numbers;
    for (double a : left.value()) {
      for (double b : right.value()) {
        auto number = fold(op.value(), a, b);
        if (!number.has_value()) {
          return std::nullopt;
        }
        numbers.push_back(number.value());
      }
    }
    return numbers;
  }

  // A single number.
  auto constant = resolved.getConstantValue();
  auto number = constant.has_value() ? constant->getNumber() : std::nullopt;
  return number.has_value() ? std::optional{std::vector{number.value()}}
                            : std::nullopt;
}

// Return the single value of `variable` from `defs` (from `VALUES` or a `BIND`
// of a constant), and `std::nullopt` if it has none or several.
std::optional<TripleComponent> getConstant(const Variable& variable,
                                           const Definitions& defs) {
  auto it = defs.constants_.find(variable);
  if (it == defs.constants_.end() || it->second.size() != 1) {
    return std::nullopt;
  }
  return it->second.front();
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
// `distance <= number` (also with swapped operands) become a `Constraint`, a
// comparison of `LANG(?x)` with constants becomes a `LangEquals`, everything
// else is `Opaque`.
Conjunct toConjunct(const SparqlExpression& expression,
                    const Definitions& defs) {
  if (auto langEquals = toLangEquals(expression, defs)) {
    return std::move(langEquals).value();
  }

  // Match `distance op number` for a distance in a unit of length, also with
  // swapped operands (`500 >= distance` is `distance <= 500`), return `Opaque`
  // for every other expression.
  auto comparison = getLessComparison(expression);
  if (!comparison.has_value()) {
    return Opaque{&expression};
  }
  auto [op, isSwapped] = comparison.value();
  const auto& distance = *expression.children()[isSwapped ? 1 : 0];
  const auto& number = *expression.children()[isSwapped ? 0 : 1];

  auto call = sparqlExpression::getGeoDistanceExpressionParameters(
      resolve(distance, defs), [&defs](const Variable& variable) {
        return getConstant(variable, defs);
      });
  auto numbers = getNumbers(number, defs);
  if (!call.has_value() || !ad_utility::detail::isLengthUnit(call->unit_) ||
      !numbers.has_value()) {
    return Opaque{&expression};
  }

  // Convert the largest number from the unit of the distance to meters.
  //
  // NOTE: For meters and kilometers, the conversion is exact (in particular, a
  // number in meters is not converted to kilometers and back, which may change
  // it). For the other units, it is exact only up to the rounding of the
  // conversion factor.
  auto [min, max] = ql::ranges::minmax(numbers.value());
  double meters = max;
  if (call->unit_ == UnitOfMeasurement::KILOMETERS) {
    meters *= 1000;
  } else if (call->unit_ != UnitOfMeasurement::METERS) {
    meters =
        ad_utility::detail::valueInUnitToKilometer(meters, call->unit_) * 1000;
  }
  return Constraint{Distance{call->left_, call->right_}, op, meters,
                    min == max};
}

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(
    const sparqlExpression::SparqlExpressionPimpl& filter,
    const Definitions& defs) {
  return ::ranges::to<std::vector>(
      filter.splitConjunction() |
      ql::views::transform([&defs](const auto& conjunct) {
        return toConjunct(*conjunct.getPimpl(), defs);
      }));
}

}  // namespace filterFacts
