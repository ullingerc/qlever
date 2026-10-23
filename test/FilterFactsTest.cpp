// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "./util/ParsedQueryTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/filterFacts/Canonicalizer.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace {

using namespace filterFacts;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;
using ::testing::VariantWith;
using valueIdComparators::Comparison;
using V = Variable;
using Literal = ad_utility::triple_component::Literal;
constexpr auto iri = ad_utility::testing::iri;

constexpr std::string_view prefixes =
    "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
    "PREFIX unit: <http://qudt.org/vocab/unit/> ";

// The two geometries used by most queries below.
constexpr std::string_view geoms = "?a <p> ?ga . ?b <p> ?gb . ";

// A parsed query together with the definitions and the canonical form of the
// FILTERs of its root group graph pattern.
struct Canonical {
  ParsedQuery query_;
  Definitions defs_;
  std::vector<Conjunct> conjuncts_;
};

// Parse `SELECT * { <body> }` and canonicalize all FILTERs of its root group.
Canonical canonical(std::string_view body) {
  Canonical result{ad_utility::testing::parseQuery(
                       absl::StrCat(prefixes, "SELECT * { ", body, " }")),
                   {},
                   {}};
  const auto& group = result.query_._rootGraphPattern;
  result.defs_ = collectDefinitions(group._graphPatterns);
  for (const auto& filter : group._filters) {
    ql::ranges::move(canonicalize(*filter.expression_.getPimpl(), result.defs_),
                     std::back_inserter(result.conjuncts_));
  }
  return result;
}

// Matcher for a `Constraint` with numeric constants on the right side.
auto numericConstraint(Term lhs, Comparison op, std::vector<double> rhs) {
  using namespace ::testing;
  auto toNumbers = [](const std::vector<TripleComponent>& constants) {
    std::vector<double> numbers;
    for (const auto& c : constants) {
      numbers.push_back(getNumericValue(c).value_or(-12345));
    }
    return numbers;
  };
  std::vector<Matcher<double>> numbers;
  for (double x : rhs) {
    numbers.push_back(DoubleNear(x, 1e-3));
  }
  return VariantWith<Constraint>(
      AllOf(Field(&Constraint::lhs_, Eq(lhs)), Field(&Constraint::op_, op),
            Field(&Constraint::rhs_,
                  ResultOf(toNumbers, ElementsAreArray(numbers)))));
}

// Matcher for a `Constraint` with arbitrary constants on the right side.
auto constraint(Term lhs, Comparison op, std::vector<TripleComponent> rhs) {
  return VariantWith<Constraint>(
      Constraint{std::move(lhs), op, std::move(rhs)});
}

// Matcher for an `Opaque` conjunct whose expression contains exactly the
// given variables (so nothing was substituted inside of it).
auto opaque(std::vector<Variable> variables) {
  using namespace ::testing;
  auto getVariables = [](const Opaque& o) {
    ad_utility::HashSet<Variable> result;
    for (const auto* var : o.expr_->containedVariables()) {
      result.insert(*var);
    }
    return std::vector<Variable>(result.begin(), result.end());
  };
  return VariantWith<Opaque>(
      ResultOf(getVariables, UnorderedElementsAreArray(variables)));
}

// The distance between the two geometries.
Term dist() { return Distance{V{"?ga"}, V{"?gb"}}; }

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, DistanceShapes) {
  auto expectDist500 = [](std::string_view body, ad_utility::source_location l =
                                                     AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(absl::StrCat(geoms, body)).conjuncts_,
                ElementsAre(numericConstraint(dist(), Comparison::LE, {500})));
  };
  // Direct, through `BIND`, through `VALUES`, and through `BIND` of a constant.
  expectDist500("FILTER(geof:distance(?ga, ?gb, unit:M) <= 500)");
  expectDist500(
      "BIND(geof:distance(?ga, ?gb, unit:M) AS ?d) FILTER(?d <= 500)");
  expectDist500(
      "VALUES ?r { 500 } FILTER(geof:distance(?ga, ?gb, unit:M) <= ?r)");
  expectDist500(
      "BIND(500 AS ?r) FILTER(geof:distance(?ga, ?gb, unit:M) <= ?r)");
  // The `VALUES` clause and the `FILTER` may be anywhere in the group.
  expectDist500(
      "FILTER(geof:distance(?ga, ?gb, unit:M) <= ?r) VALUES ?r { 500 }");
  // `BIND` and `VALUES` combined.
  expectDist500(
      "BIND(geof:distance(?ga, ?gb, unit:M) AS ?d) VALUES ?r { 500 } "
      "FILTER(?d <= ?r)");

  // Several values.
  EXPECT_THAT(
      canonical(absl::StrCat(geoms,
                             "VALUES ?r { 500 1000 } "
                             "FILTER(geof:distance(?ga, ?gb, unit:M) <= ?r)"))
          .conjuncts_,
      ElementsAre(numericConstraint(dist(), Comparison::LE, {500, 1000})));
}

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, FlipComparison) {
  auto expectOp = [](std::string_view filter, Comparison op,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(canonical(absl::StrCat(geoms, filter)).conjuncts_,
                ElementsAre(numericConstraint(dist(), op, {500})));
  };
  expectOp("FILTER(geof:metricDistance(?ga, ?gb) <= 500)", Comparison::LE);
  expectOp("FILTER(geof:metricDistance(?ga, ?gb) < 500)", Comparison::LT);
  expectOp("FILTER(500 >= geof:metricDistance(?ga, ?gb))", Comparison::LE);
  expectOp("FILTER(500 > geof:metricDistance(?ga, ?gb))", Comparison::LT);
  expectOp("FILTER(500 <= geof:metricDistance(?ga, ?gb))", Comparison::GE);
  expectOp("FILTER(500 < geof:metricDistance(?ga, ?gb))", Comparison::GT);
  expectOp("FILTER(500 = geof:metricDistance(?ga, ?gb))", Comparison::EQ);
  expectOp("FILTER(500 != geof:metricDistance(?ga, ?gb))", Comparison::NE);
  expectOp(
      "BIND(geof:metricDistance(?ga, ?gb) AS ?d) VALUES ?r { 500 } "
      "FILTER(?r >= ?d)",
      Comparison::LE);
}

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, SplitConjunction) {
  auto c = canonical(absl::StrCat(
      geoms,
      "FILTER(?a = <x> && (geof:metricDistance(?ga, ?gb) <= 500 && "
      "STRLEN(?b) > 3)) FILTER(?b != <y>)"));
  EXPECT_THAT(c.conjuncts_,
              ElementsAre(constraint(V{"?a"}, Comparison::EQ, {iri("<x>")}),
                          numericConstraint(dist(), Comparison::LE, {500}),
                          VariantWith<Constraint>(::testing::Field(
                              &Constraint::op_, Comparison::GT)),
                          constraint(V{"?b"}, Comparison::NE, {iri("<y>")})));
  // A disjunction is not split.
  EXPECT_THAT(canonical("?a <p> ?b FILTER(?a = <x> || ?b = <y>)").conjuncts_,
              ElementsAre(VariantWith<Opaque>(::testing::_)));
}

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, UnitNormalization) {
  auto expectMeters = [](std::string_view filter, double meters,
                         ad_utility::source_location l =
                             AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(
        canonical(absl::StrCat(geoms, filter)).conjuncts_,
        ElementsAre(numericConstraint(dist(), Comparison::LE, {meters})));
  };
  expectMeters("FILTER(geof:distance(?ga, ?gb) <= 0.5)", 500);
  expectMeters("FILTER(geof:distance(?ga, ?gb, unit:KiloM) <= 0.5)", 500);
  expectMeters("FILTER(geof:distance(?ga, ?gb, unit:M) <= 500)", 500);
  expectMeters("FILTER(geof:distance(?ga, ?gb, unit:MI) <= 1)", 1609.344);
  expectMeters("FILTER(geof:metricDistance(?ga, ?gb) <= 500)", 500);
  expectMeters("VALUES ?r { 0.5 } FILTER(geof:distance(?ga, ?gb) <= ?r)", 500);
  expectMeters("FILTER(0.5 >= geof:distance(?ga, ?gb))", 500);

  // Non-numeric constants are kept as they are.
  EXPECT_THAT(
      canonical(absl::StrCat(geoms, "FILTER(geof:distance(?ga, ?gb) <= \"x\")"))
          .conjuncts_,
      ElementsAre(constraint(dist(), Comparison::LE,
                             {Literal::literalWithoutQuotes("x")})));
}

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, ConstantFolding) {
  auto expectMeters = [](std::string_view filter, double meters,
                         ad_utility::source_location l =
                             AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_THAT(
        canonical(absl::StrCat(geoms, filter)).conjuncts_,
        ElementsAre(numericConstraint(dist(), Comparison::LE, {meters})));
  };
  expectMeters("FILTER(geof:metricDistance(?ga, ?gb) <= 250 * 2)", 500);
  expectMeters("FILTER(geof:metricDistance(?ga, ?gb) <= 250 + 250)", 500);
  expectMeters("FILTER(geof:metricDistance(?ga, ?gb) <= 750 - 250)", 500);
  expectMeters("FILTER(geof:metricDistance(?ga, ?gb) <= 1000 / 2)", 500);
  expectMeters("FILTER(geof:distance(?ga, ?gb) <= 0.25 * 2)", 500);
  expectMeters("VALUES ?r { 0.25 } FILTER(geof:distance(?ga, ?gb) <= ?r * 2)",
               500);
  expectMeters(
      "VALUES ?r { 0.25 } BIND(?r * 2 AS ?s) "
      "FILTER(geof:distance(?ga, ?gb) <= ?s)",
      500);

  // Not folded: a division by zero, non-numeric operands, and several values
  // on both sides.
  for (std::string_view filter :
       {"FILTER(geof:metricDistance(?ga, ?gb) <= 1 / 0)",
        "FILTER(geof:metricDistance(?ga, ?gb) <= \"1\" + 1)",
        "VALUES ?r { 1 2 } VALUES ?s { 1 2 } "
        "FILTER(geof:metricDistance(?ga, ?gb) <= ?r + ?s)"}) {
    EXPECT_THAT(canonical(absl::StrCat(geoms, filter)).conjuncts_,
                ElementsAre(VariantWith<Opaque>(::testing::_)))
        << filter;
  }
}

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, NoSubstitutionInsideBoundOrExists) {
  auto bind = "BIND(geof:metricDistance(?ga, ?gb) AS ?d) ";
  // `BOUND(?d)` stays as it is, the other conjunct is substituted.
  EXPECT_THAT(
      canonical(absl::StrCat(geoms, bind, "FILTER(BOUND(?d) && ?d <= 500)"))
          .conjuncts_,
      ElementsAre(opaque({V{"?d"}}),
                  numericConstraint(dist(), Comparison::LE, {500})));
  EXPECT_THAT(
      canonical(absl::StrCat(geoms, bind, "FILTER(!BOUND(?d) || ?d <= 500)"))
          .conjuncts_,
      ElementsAre(opaque({V{"?d"}})));
  EXPECT_THAT(
      canonical(absl::StrCat(geoms, bind,
                             "FILTER(EXISTS { ?d <q> ?z } && ?d <= 500)"))
          .conjuncts_,
      ElementsAre(VariantWith<Opaque>(::testing::_),
                  numericConstraint(dist(), Comparison::LE, {500})));
}

// _____________________________________________________________________________
TEST(FilterFactsCanonicalizer, Lang) {
  auto en = Literal::literalWithoutQuotes("en");
  auto de = Literal::literalWithoutQuotes("de");
  const Term lang = Lang{V{"?y"}};
  auto expectLang =
      [&](std::string_view body, std::vector<TripleComponent> langs,
          ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(
            canonical(absl::StrCat("VALUES ?y { \"a\"@en } ", body)).conjuncts_,
            ElementsAre(constraint(lang, Comparison::EQ, langs)));
      };
  expectLang("FILTER(LANG(?y) = \"en\")", {en});
  expectLang("FILTER(\"en\" = LANG(?y))", {en});
  expectLang("BIND(LANG(?y) AS ?l) FILTER(?l = \"en\")", {en});
  expectLang("FILTER(LANG(?y) IN (\"en\", \"de\"))", {en, de});
  expectLang("BIND(LANG(?y) AS ?l) FILTER(?l IN (\"en\", \"de\"))", {en, de});
  expectLang("FILTER(LANG(?y) = \"en\" || LANG(?y) = \"de\")", {en, de});
  expectLang("VALUES ?l { \"en\" \"de\" } FILTER(LANG(?y) = ?l)", {en, de});

  // Different variables or operators are not merged.
  EXPECT_THAT(canonical("VALUES (?y ?z) { (\"a\"@en \"b\"@de) } "
                        "FILTER(LANG(?y) = \"en\" || LANG(?z) = \"de\")")
                  .conjuncts_,
              ElementsAre(VariantWith<Opaque>(::testing::_)));
  EXPECT_THAT(canonical("VALUES ?y { \"a\"@en } "
                        "FILTER(LANG(?y) = \"en\" || LANG(?y) != \"de\")")
                  .conjuncts_,
              ElementsAre(VariantWith<Opaque>(::testing::_)));
}

// _____________________________________________________________________________
TEST(FilterFactsDefinitions, CollectDefinitions) {
  auto defs = [](std::string_view body) { return canonical(body).defs_; };
  auto bind = "BIND(geof:metricDistance(?ga, ?gb) AS ?d) ";

  // Positive cases.
  auto d = defs(absl::StrCat(geoms, bind, "VALUES (?r ?s) { (1 2) (3 4) }"));
  EXPECT_TRUE(d.expressions_.contains(V{"?d"}));
  EXPECT_THAT(
      d.constants_.at(V{"?r"}),
      ElementsAre(TripleComponent{int64_t{1}}, TripleComponent{int64_t{3}}));
  EXPECT_THAT(
      d.constants_.at(V{"?s"}),
      ElementsAre(TripleComponent{int64_t{2}}, TripleComponent{int64_t{4}}));
  // Inputs bound by `VALUES` before the `BIND`.
  EXPECT_TRUE(defs("VALUES ?x { 1 } BIND(?x + 1 AS ?y)")
                  .expressions_.contains(V{"?y"}));

  // `?gb` is not bound before the `BIND`.
  EXPECT_THAT(
      defs(absl::StrCat("?a <p> ?ga . ", bind, "?b <p> ?gb .")).expressions_,
      IsEmpty());
  // `?gb` is only bound in a nested group, an `OPTIONAL`, or another `BIND`.
  for (std::string_view binding :
       {"{ ?b <p> ?gb }", "OPTIONAL { ?b <p> ?gb }", "BIND(<x> AS ?gb)"}) {
    EXPECT_THAT(defs(absl::StrCat("?a <p> ?ga . ", binding, bind)).expressions_,
                ::testing::Not(::testing::Contains(::testing::Key(V{"?d"}))))
        << binding;
  }
  // Non-deterministic expressions, `NOW()` and `EXISTS`.
  for (std::string_view expr :
       {"RAND()", "?x + RAND()", "UUID()", "STRUUID()", "BNODE()", "NOW()",
        "YEAR(NOW())", "EXISTS { ?x <q> ?z }"}) {
    EXPECT_THAT(
        defs(absl::StrCat("?x <p> ?y BIND(", expr, " AS ?b)")).expressions_,
        IsEmpty())
        << expr;
  }
  // `UNDEF` in the column of the variable, but not in other columns.
  auto undef = defs("VALUES (?r ?s) { (1 UNDEF) (2 3) }");
  EXPECT_THAT(
      undef.constants_.at(V{"?r"}),
      ElementsAre(TripleComponent{int64_t{1}}, TripleComponent{int64_t{2}}));
  EXPECT_FALSE(undef.constants_.contains(V{"?s"}));
  // Definitions inside of nested groups, `OPTIONAL`, `MINUS`, `UNION` or
  // subqueries are not visible.
  for (std::string_view body :
       {"{ VALUES ?r { 1 } BIND(1 AS ?q) }",
        "?x <p> ?y OPTIONAL { VALUES ?r { 1 } BIND(1 AS ?q) }",
        "?x <p> ?y MINUS { VALUES ?r { 1 } BIND(1 AS ?q) }",
        "{ VALUES ?r { 1 } BIND(1 AS ?q) } UNION { ?x <p> ?y }",
        "{ SELECT * { VALUES ?r { 1 } BIND(1 AS ?q) } }"}) {
    auto nested = defs(body);
    EXPECT_THAT(nested.constants_, IsEmpty()) << body;
    EXPECT_THAT(nested.expressions_, IsEmpty()) << body;
  }
  // A `VALUES` clause after the `WHERE` clause is joined with the complete
  // `WHERE` clause, which becomes a nested group (or, with `GROUP BY`, it is
  // stored separately). Its values are not visible in the `WHERE` clause.
  auto trailing = canonical("?x <p> ?y FILTER(?y <= ?r) } VALUES ?r { 1 ");
  const auto& root = trailing.query_._rootGraphPattern;
  EXPECT_THAT(root._filters, IsEmpty());
  ASSERT_EQ(root._graphPatterns.size(), 2);
  const auto& where =
      std::get<parsedQuery::GroupGraphPattern>(root._graphPatterns[0])._child;
  EXPECT_EQ(where._filters.size(), 1);
  EXPECT_THAT(collectDefinitions(where._graphPatterns).constants_, IsEmpty());
  auto groupBy = ad_utility::testing::parseQuery(
      "SELECT ?r { ?x <p> ?r FILTER(?r <= 3) } GROUP BY ?r VALUES ?r { 1 }");
  EXPECT_TRUE(groupBy.postQueryValuesClause_.has_value());
  EXPECT_THAT(
      collectDefinitions(groupBy._rootGraphPattern._graphPatterns).constants_,
      IsEmpty());
}

}  // namespace
