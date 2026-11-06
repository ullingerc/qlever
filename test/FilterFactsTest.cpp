// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <cmath>
#include <limits>

#include "./util/GTestHelpers.h"
#include "./util/ParsedQueryTestHelpers.h"
#include "engine/filterFacts/Canonicalizer.h"
#include "engine/filterFacts/Recognizers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"

namespace {

using namespace filterFacts;
using ::testing::ElementsAre;
using ::testing::VariantWith;
using valueIdComparators::Comparison;
using V = Variable;
using Literal = ad_utility::triple_component::Literal;

// A parsed query and the canonical form of the FILTERs of its root group
// graph pattern (which point into the query).
struct Canonical {
  ParsedQuery query_;
  std::vector<Conjunct> conjuncts_;
};

// Parse `SELECT * { ?a <p> ?ga . ?b <p> ?gb . <definitions> FILTER(<filter>)
// }` and canonicalize its FILTER with the definitions of the group.
Canonical canonical(std::string_view filter,
                    std::string_view definitions = "") {
  Canonical result{
      ad_utility::testing::parseQuery(absl::StrCat(
          "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
          "PREFIX unit: <http://qudt.org/vocab/unit/> "
          "SELECT * { ?a <p> ?ga . ?b <p> ?gb . ",
          definitions, " FILTER(", filter, ") }")),
      {}};
  const auto& group = result.query_._rootGraphPattern;
  auto defs = collectDefinitions(group._graphPatterns);
  for (const auto& f : group._filters) {
    ql::ranges::move(canonicalize(*f.expression_.getPimpl(), defs),
                     std::back_inserter(result.conjuncts_));
  }
  return result;
}

// Matcher for a `Constraint` `distance op meters` (compared with a tolerance,
// because the factors of the conversion of units are rounded).
auto constraint(Comparison op, double meters, bool exact = true) {
  using namespace ::testing;
  return VariantWith<Constraint>(
      AllOf(Field(&Constraint::lhs_, Eq(Distance{V{"?ga"}, V{"?gb"}})),
            Field(&Constraint::op_, op),
            Field(&Constraint::rhs_, DoubleNear(meters, 1e-3)),
            Field(&Constraint::exact_, exact)));
}

// Test that `distance < number` and `distance <= number` become a
// `Constraint` with the number in meters, also with swapped operands and in a
// nested `&&`.
TEST(FilterFactsCanonicalizer, Distance) {
  // Expect that `filter` is a single `Constraint` `distance op meters`.
  auto expect = [](std::string_view filter, Comparison op, double meters,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(filter).conjuncts_,
                ElementsAre(constraint(op, meters)));
  };

  // Different distance functions and units.
  expect("geof:distance(?ga, ?gb, unit:M) <= 500", Comparison::LE, 500);
  expect("geof:distance(?ga, ?gb) <= 0.5", Comparison::LE, 500);
  expect("geof:metricDistance(?ga, ?gb) < 500", Comparison::LT, 500);
  expect("geof:distance(?ga, ?gb, unit:MI) < 1", Comparison::LT, 1609.344);

  // Swapped operands.
  expect("500 >= geof:metricDistance(?ga, ?gb)", Comparison::LE, 500);
  expect("0.5 > geof:distance(?ga, ?gb)", Comparison::LT, 500);

  // A number in meters is exactly the number of the FILTER (whereas
  // `1001 / 1000 * 1000` and `63.7 / 1000 * 1000` are not), and a number in
  // kilometers is exactly 1000 times it.
  for (auto [filter, factor] : std::vector<std::pair<std::string_view, double>>{
           {"geof:metricDistance(?ga, ?gb) <= 1001", 1},
           {"geof:distance(?ga, ?gb, unit:M) < 63.7", 1},
           {"geof:distance(?ga, ?gb) <= 1.1", 1000}}) {
    auto result = canonical(filter);
    const auto& comparison =
        *result.query_._rootGraphPattern._filters[0].expression_.getPimpl();
    auto number = dynamic_cast<const sparqlExpression::IdExpression&>(
                      *comparison.children()[1])
                      .value();
    double expected = number.getDatatype() == Datatype::Int
                          ? static_cast<double>(number.getInt())
                          : number.getDouble();
    ASSERT_EQ(result.conjuncts_.size(), 1u);
    EXPECT_EQ(std::get<Constraint>(result.conjuncts_[0]).rhs_,
              expected * factor)
        << filter;
  }

  // Each conjunct of `&&` is canonicalized on its own, and the `Opaque` one
  // points to its subexpression.
  auto result = canonical(
      "geof:metricDistance(?ga, ?gb) <= 500 && "
      "(?a != ?b && geof:metricDistance(?ga, ?gb) < 400)");
  const auto& root =
      *result.query_._rootGraphPattern._filters[0].expression_.getPimpl();
  const auto* notEqual = root.children()[1]->children()[0].get();
  EXPECT_THAT(result.conjuncts_,
              ElementsAre(constraint(Comparison::LE, 500),
                          VariantWith<Opaque>(Opaque{notEqual}),
                          constraint(Comparison::LT, 400)));
}

// Test that a variable is replaced by its definition from `VALUES` or `BIND`
// (for the distance, the number and the unit), and that a `Constraint` with
// different numbers in different rows has the largest one and is not exact.
TEST(FilterFactsCanonicalizer, Definitions) {
  auto expect = [](std::string_view definitions, std::string_view filter,
                   Comparison op, double meters, bool exact = true,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(filter, definitions).conjuncts_,
                ElementsAre(constraint(op, meters, exact)));
  };
  expect("VALUES ?r { 500 }", "geof:metricDistance(?ga, ?gb) <= ?r",
         Comparison::LE, 500);
  expect("BIND(0.5 AS ?r)", "geof:distance(?ga, ?gb) < ?r", Comparison::LT,
         500);
  expect("VALUES ?u { unit:MI }", "geof:distance(?ga, ?gb, ?u) <= 1",
         Comparison::LE, 1609.344);
  expect("BIND(unit:MI AS ?u)", "geof:distance(?ga, ?gb, ?u) <= 1",
         Comparison::LE, 1609.344);
  expect("BIND(geof:metricDistance(?ga, ?gb) AS ?d)", "?d <= 500",
         Comparison::LE, 500);
  expect("VALUES ?r { 1000 500 }", "geof:metricDistance(?ga, ?gb) <= ?r",
         Comparison::LE, 1000, false);
}

// Test that a number computed with `+`, `-`, `*` or `/` is folded, unless it
// is a division by zero, an operand is not a number, or both operands have
// several values.
TEST(FilterFactsCanonicalizer, ConstantFolding) {
  auto expect = [](std::string_view definitions, std::string_view filter,
                   double meters, bool exact = true,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(filter, definitions).conjuncts_,
                ElementsAre(constraint(Comparison::LE, meters, exact)));
  };
  expect("", "geof:metricDistance(?ga, ?gb) <= 250 * 2", 500);
  expect("", "geof:metricDistance(?ga, ?gb) <= 250 + 250", 500);
  expect("", "geof:metricDistance(?ga, ?gb) <= 750 - 250", 500);
  expect("", "geof:metricDistance(?ga, ?gb) <= 1000 / 2", 500);
  expect("", "geof:distance(?ga, ?gb) <= 0.25 * 2", 500);
  expect("VALUES ?r { 0.25 }", "geof:distance(?ga, ?gb) <= ?r * 2", 500);
  expect("VALUES ?r { 0.25 } BIND(?r * 2 AS ?s)",
         "geof:distance(?ga, ?gb) <= ?s", 500);
  expect("VALUES ?r { 100 250 }", "geof:metricDistance(?ga, ?gb) <= ?r * 2",
         500, false);

  for (auto [definitions, filter] :
       std::vector<std::pair<std::string_view, std::string_view>>{
           {"", "geof:metricDistance(?ga, ?gb) <= 1 / 0"},
           {"", "geof:metricDistance(?ga, ?gb) <= \"1\" + 1"},
           {"VALUES ?r { 1 2 } VALUES ?s { 1 2 }",
            "geof:metricDistance(?ga, ?gb) <= ?r + ?s"}}) {
    EXPECT_THAT(canonical(filter, definitions).conjuncts_,
                ElementsAre(VariantWith<Opaque>(::testing::_)))
        << filter;
  }
}

// Test that a comparison of `LANG(?x)` with constants (also with `IN`, `||`,
// swapped operands, and definitions) becomes a `LangEquals`, which is not
// exact if a constant has different values in different rows.
TEST(FilterFactsCanonicalizer, Lang) {
  // Expect that `filter` (with `?y` bound by `VALUES` and the given
  // `definitions`) is a single `LangEquals` for `?y`.
  auto expect = [](std::string_view definitions, std::string_view filter,
                   std::vector<std::string_view> langs, bool exact = true,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    std::vector<TripleComponent> rhs;
    for (auto lang : langs) {
      rhs.emplace_back(Literal::literalWithoutQuotes(lang));
    }
    EXPECT_THAT(
        canonical(filter, absl::StrCat("VALUES ?y { \"a\"@en } ", definitions))
            .conjuncts_,
        ElementsAre(VariantWith<LangEquals>(LangEquals{V{"?y"}, rhs, exact})));
  };
  expect("", "LANG(?y) = \"en\"", {"en"});
  expect("", "\"en\" = LANG(?y)", {"en"});
  expect("BIND(LANG(?y) AS ?l)", "?l = \"en\"", {"en"});
  expect("BIND(\"en\" AS ?l)", "LANG(?y) = ?l", {"en"});
  expect("", "LANG(?y) IN (\"en\", \"de\")", {"en", "de"});
  expect("BIND(LANG(?y) AS ?l)", "?l IN (\"en\", \"de\")", {"en", "de"});
  expect("", "LANG(?y) = \"en\" || LANG(?y) = \"de\"", {"en", "de"});
  expect("VALUES ?l { \"en\" \"de\" }", "LANG(?y) = ?l", {"en", "de"}, false);

  // Different variables or other comparisons.
  for (std::string_view filter :
       {"LANG(?y) = \"en\" || LANG(?ga) = \"de\"",
        "LANG(?y) = \"en\" || LANG(?y) != \"de\"", "LANG(?y) != \"en\""}) {
    EXPECT_THAT(canonical(filter, "VALUES ?y { \"a\"@en }").conjuncts_,
                ElementsAre(VariantWith<Opaque>(::testing::_)))
        << filter;
  }
}

// Test that only the columns of `VALUES` without `UNDEF`, the `BIND`s of
// constants and the `BIND`s whose target is equivalent to their expression are
// definitions.
TEST(FilterFactsCanonicalizer, CollectDefinitions) {
  using ::testing::Key;
  using ::testing::UnorderedElementsAre;
  auto query = ad_utility::testing::parseQuery(
      "SELECT * { ?a <p> ?ga . BIND(?ga AS ?x) BIND(STR(?x) AS ?u) "
      "BIND(?gb AS ?y) ?b <p> ?gb . "
      "BIND(RAND() AS ?z) BIND(NOW() AS ?n) BIND(?ga AS ?w) ?w <q> ?o . "
      "VALUES (?r ?s) { (1 UNDEF) } BIND(500 AS ?c) ?c <q> ?o2 }");
  auto defs = collectDefinitions(query._rootGraphPattern._graphPatterns);

  // `?x` is only used by a later `BIND`, `?u` uses `?x`, which may be unbound,
  // `?y` is bound before `?gb`, `?z` and `?n` are not deterministic, and `?w`
  // is bound again by a later triple.
  EXPECT_THAT(defs.expressions_, UnorderedElementsAre(Key(V{"?x"})));

  // The target of a `BIND` of a constant has this value, even if a later
  // triple binds it again.
  EXPECT_THAT(defs.constants_,
              UnorderedElementsAre(Key(V{"?r"}), Key(V{"?c"})));
}

// Test that every other expression stays `Opaque`.
TEST(FilterFactsCanonicalizer, Opaque) {
  for (std::string_view filter :
       {"geof:metricDistance(?ga, ?gb) > 500",
        "500 <= geof:metricDistance(?ga, ?gb)",
        "geof:metricDistance(?ga, ?gb) <= \"500\"",
        "geof:metricDistance(?ga, ?gb) <= ?a",
        "geof:metricDistance(?ga, ?gb) + 1 <= 500", "?a <= 500", "BOUND(?a)",
        "geof:metricDistance(?ga, ?gb) <= 500 || ?a = 1",
        "geof:distance(?ga, ?gb, unit:M2) <= 500"}) {
    auto result = canonical(filter);
    EXPECT_THAT(result.conjuncts_,
                ElementsAre(VariantWith<Opaque>(
                    Opaque{result.query_._rootGraphPattern._filters[0]
                               .expression_.getPimpl()})))
        << filter;
  }
}

// Test that `distance <= c` and `distance < c` for a finite number `c >= 0`
// between two different geometries (at least one of them a variable) are
// recognized as `DistanceBound`.
TEST(FilterFactsRecognizers, DistanceBound) {
  // Recognize the `DistanceBound` of the single conjunct of `filter` (while
  // the parsed query is alive), and a constant point.
  auto recognize = [](std::string_view filter) {
    auto result = canonical(filter);
    AD_CORRECTNESS_CHECK(result.conjuncts_.size() == 1);
    return recognizeDistanceBound(result.conjuncts_[0]);
  };
  auto point = std::string{
      "\"POINT(7.8 48.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};

  // `<=` and `<` (where the maximum distance is the largest `double` below
  // the number).
  EXPECT_THAT(recognize("geof:distance(?ga, ?gb, unit:M) <= 500"),
              ::testing::Optional(DistanceBound{V{"?ga"}, V{"?gb"}, 500}));

  // A `DistanceBound` from a `Constraint` that is not exact is not exact.
  auto conjuncts =
      canonical("geof:metricDistance(?ga, ?gb) <= ?r", "VALUES ?r { 500 1000 }")
          .conjuncts_;
  EXPECT_THAT(
      recognizeDistanceBound(conjuncts.at(0)),
      ::testing::Optional(DistanceBound{V{"?ga"}, V{"?gb"}, 1000, false}));
  EXPECT_THAT(
      recognize("geof:metricDistance(?ga, ?gb) < 500"),
      ::testing::Optional(DistanceBound{
          V{"?ga"}, V{"?gb"},
          std::nextafter(500.0, -std::numeric_limits<double>::infinity())}));
  EXPECT_THAT(
      recognize("geof:metricDistance(?ga, ?gb) < 1001"),
      ::testing::Optional(::testing::Field(
          &DistanceBound::maxMeters_,
          std::nextafter(1001.0, -std::numeric_limits<double>::infinity()))));

  // A constant geometry on one side.
  EXPECT_THAT(
      recognize(absl::StrCat("geof:metricDistance(?ga, ", point, ") <= 500")),
      ::testing::Optional(
          ::testing::Field(&DistanceBound::a_, TripleComponent{V{"?ga"}})));

  // A negative, infinite or NaN maximum distance, the same variable or two
  // constants, or no `Constraint` at all.
  auto xsdDouble = std::string{"^^<http://www.w3.org/2001/XMLSchema#double>"};
  for (std::string filter :
       {std::string{"geof:metricDistance(?ga, ?gb) <= -1"},
        std::string{"geof:metricDistance(?ga, ?gb) < 0"},
        absl::StrCat("geof:metricDistance(?ga, ?gb) <= \"INF\"", xsdDouble),
        absl::StrCat("geof:metricDistance(?ga, ?gb) <= \"NaN\"", xsdDouble),
        std::string{"geof:metricDistance(?ga, ?ga) <= 500"},
        absl::StrCat("geof:metricDistance(", point, ", ", point, ") <= 500"),
        std::string{"BOUND(?a)"}}) {
    EXPECT_EQ(recognize(filter), std::nullopt) << filter;
  }
}

// Test that a `LangEquals` with plain string literals is recognized as
// `LangIn`.
TEST(FilterFactsRecognizers, LangIn) {
  auto recognize = [](std::string_view filter,
                      std::string_view definitions = "") {
    auto conjuncts =
        canonical(filter, absl::StrCat("VALUES ?y { \"a\"@en } ", definitions))
            .conjuncts_;
    AD_CORRECTNESS_CHECK(conjuncts.size() == 1);
    return recognizeLangIn(conjuncts[0]);
  };
  EXPECT_THAT(recognize("LANG(?y) IN (\"en\", \"de\")"),
              ::testing::Optional(LangIn{V{"?y"}, {"en", "de"}}));
  EXPECT_THAT(recognize("LANG(?y) = "
                        "\"en\"^^<http://www.w3.org/2001/XMLSchema#string>"),
              ::testing::Optional(LangIn{V{"?y"}, {"en"}}));

  // A `LangIn` from a `LangEquals` that is not exact is not exact.
  EXPECT_THAT(recognize("LANG(?y) = ?l", "VALUES ?l { \"en\" \"de\" }"),
              ::testing::Optional(LangIn{V{"?y"}, {"en", "de"}, false}));

  // A literal with a language tag or a datatype, no literal, no constant at
  // all, or no `LangEquals`.
  for (std::string_view filter :
       {"LANG(?y) = \"en\"@de", "LANG(?y) = \"en\"^^<x>", "LANG(?y) = <en>",
        "LANG(?y) IN ()", "LANGMATCHES(LANG(?y), \"en\")"}) {
    EXPECT_EQ(recognize(filter), std::nullopt) << filter;
  }
}

}  // namespace
