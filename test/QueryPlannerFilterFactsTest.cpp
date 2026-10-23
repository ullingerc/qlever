// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include "./QueryPlannerTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/SpatialJoin.h"

// Tests for the consumers of the filter fact recognition in the
// `QueryPlanner`, see `engine/filterFacts` and
// `docs/experimental/filter-fact-recognition.md`.

namespace {

namespace h = queryPlannerTestHelpers;
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using V = Variable;

constexpr std::string_view prefixes =
    "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
    "PREFIX unit: <http://qudt.org/vocab/unit/> "
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

// Four points at increasing distances from the first one (about 0 m, 333 m,
// 667 m, and 2.2 km).
constexpr std::string_view turtle =
    "<a1> <p> \"POINT(7.8 48.0)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<a2> <p> \"POINT(7.8 48.003)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<a3> <p> \"POINT(7.8 48.006)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<a4> <p> \"POINT(7.8 48.02)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . ";

constexpr std::string_view geoms = "?a <p> ?ga . ?b <p> ?gb . ";

// `SELECT * { <body> }` with the prefixes from above.
std::string query(std::string_view body) {
  return absl::StrCat(prefixes, "SELECT * { ", body, " }");
}

// Set the switch for the filter fact recognition for the lifetime of the
// returned object.
auto factsEnabled(bool enabled) {
  return setRuntimeParameterForTest<
      &RuntimeParameters::enableFilterFactRecognition_>(enabled);
}

// Collect all operations of type `T` in `qet`.
template <typename T>
void collect(const QueryExecutionTree& qet, std::vector<const T*>& result) {
  const auto& op = *qet.getRootOperation();
  if (const auto* t = dynamic_cast<const T*>(&op)) {
    result.push_back(t);
  }
  for (const auto* child : op.getChildren()) {
    collect(*child, result);
  }
}

// Plan `body` and return the maximum distances of all its `SpatialJoin`s and
// the descriptors of all its `Filter`s.
struct PlanSummary {
  std::vector<double> spatialJoinMaxDists_;
  std::vector<std::string> filters_;
};
PlanSummary plan(std::string_view body) {
  auto qet = h::parseAndPlan(query(body),
                             ad_utility::testing::getQec(std::string{turtle}));
  std::vector<const SpatialJoin*> spatialJoins;
  collect(*qet, spatialJoins);
  std::vector<const Filter*> filters;
  collect(*qet, filters);
  PlanSummary result;
  for (const auto* sj : spatialJoins) {
    EXPECT_TRUE(sj->isConstructed());
    result.spatialJoinMaxDists_.push_back(sj->getMaxDist().value_or(-1));
  }
  for (const auto* filter : filters) {
    result.filters_.push_back(filter->getDescriptor());
  }
  return result;
}

// _____________________________________________________________________________
TEST(QueryPlannerFilterFacts, DistanceShapes) {
  // Each shape with the maximum distance of the resulting `SpatialJoin` and
  // the original FILTER, which must stay in the plan.
  std::vector<std::tuple<std::string, double, std::string>> shapes{
      {"FILTER(geof:distance(?ga, ?gb, unit:M) <= 500)", 500,
       "geof:distance(?ga, ?gb, unit:M) <= 500"},
      {"BIND(geof:distance(?ga, ?gb, unit:M) AS ?d) FILTER(?d <= 500)", 500,
       "?d <= 500"},
      {"VALUES ?r { 500 } FILTER(geof:distance(?ga, ?gb, unit:M) <= ?r)", 500,
       "geof:distance(?ga, ?gb, unit:M) <= ?r"},
      {"FILTER(500 >= geof:metricDistance(?ga, ?gb))", 500,
       "500 >= geof:metricDistance(?ga, ?gb)"},
      {"FILTER(geof:distance(?ga, ?gb) < 0.5)", 500,
       "geof:distance(?ga, ?gb) < 0.5"},
      {"VALUES ?r { 500 1000 } FILTER(geof:distance(?ga, ?gb, unit:M) <= ?r)",
       1000, "geof:distance(?ga, ?gb, unit:M) <= ?r"},
      {"BIND(geof:distance(?ga, ?gb, unit:M) AS ?d) VALUES ?r { 500 } "
       "FILTER(?d <= ?r)",
       500, "?d <= ?r"},
  };
  for (const auto& [shape, maxDist, filter] : shapes) {
    auto body = absl::StrCat(geoms, shape);
    {
      auto cleanup = factsEnabled(true);
      auto summary = plan(body);
      EXPECT_THAT(summary.spatialJoinMaxDists_,
                  ElementsAre(DoubleNear(maxDist, 1e-6)))
          << shape;
      EXPECT_THAT(summary.filters_, ElementsAre(::testing::HasSubstr(filter)))
          << shape;
    }
    // With the switch off, only the direct shape is rewritten (and then the
    // FILTER is replaced by the `SpatialJoin`).
    auto cleanup = factsEnabled(false);
    auto summary = plan(body);
    if (ql::starts_with(shape,
                        "FILTER(geof:distance(?ga, ?gb, unit:M) <= 500)")) {
      EXPECT_THAT(summary.spatialJoinMaxDists_, ElementsAre(500));
      EXPECT_THAT(summary.filters_, IsEmpty());
    } else if (shape.find("BIND") != std::string::npos ||
               shape.find("VALUES") != std::string::npos) {
      EXPECT_THAT(summary.spatialJoinMaxDists_, IsEmpty()) << shape;
    }
  }
}

// _____________________________________________________________________________
TEST(QueryPlannerFilterFacts, PlanMatchesExactly) {
  auto cleanup = factsEnabled(true);
  auto scan = h::IndexScanFromStrings;
  // The original FILTER is applied on top of the `SpatialJoin`.
  h::expect(query(absl::StrCat(
                geoms, "FILTER(geof:distance(?ga, ?gb, unit:M) <= 500)")),
            h::Filter("geof:distance(?ga, ?gb, unit:M) <= 500",
                      h::spatialJoinFilterSubstitute(
                          500, -1, V{"?ga"}, V{"?gb"}, std::nullopt,
                          PayloadVariables::all(),
                          SpatialJoinAlgorithm::LIBSPATIALJOIN,
                          SpatialJoinType::WITHIN_DIST, std::nullopt,
                          scan("?a", "<p>", "?ga"), scan("?b", "<p>", "?gb"))));
}

// _____________________________________________________________________________
TEST(QueryPlannerFilterFacts, NoRewrite) {
  auto cleanup = factsEnabled(true);
  auto dist = "geof:metricDistance(?ga, ?gb)";
  for (std::string body : {
           // `?gb` is not bound when the `BIND` is evaluated.
           absl::StrCat("?a <p> ?ga . BIND(", dist, " AS ?d) ?b <p> ?gb . ",
                        "FILTER(?d <= 500)"),
           // `UNDEF` in `VALUES`.
           absl::StrCat(geoms, "VALUES ?r { 500 UNDEF } FILTER(", dist,
                        " <= ?r)"),
           // Non-deterministic `BIND`s.
           absl::StrCat(geoms, "BIND(", dist, " + RAND() AS ?d) ",
                        "FILTER(?d <= 500)"),
           absl::StrCat(geoms, "BIND(500 + YEAR(NOW()) * 0 AS ?r) FILTER(",
                        dist, " <= ?r)"),
           // Definitions in another scope.
           absl::StrCat("{ ", geoms, "BIND(", dist, " AS ?d) } ",
                        "FILTER(?d <= 500)"),
           absl::StrCat(geoms, "{ VALUES ?r { 500 } } FILTER(", dist,
                        " <= ?r)"),
           absl::StrCat(geoms, "OPTIONAL { VALUES ?r { 500 } } FILTER(", dist,
                        " <= ?r)"),
           absl::StrCat(geoms, "{ BIND(500 AS ?r) } UNION { BIND(1 AS ?r) } ",
                        "FILTER(", dist, " <= ?r)"),
           absl::StrCat(geoms, "{ SELECT ?r { VALUES ?r { 500 } } } FILTER(",
                        dist, " <= ?r)"),
           // A string instead of a number.
           absl::StrCat(geoms, "VALUES ?r { \"500\" } FILTER(", dist,
                        " <= ?r)"),
       }) {
    EXPECT_THAT(plan(body).spatialJoinMaxDists_, IsEmpty()) << body;
  }

  // A trailing `VALUES` clause is joined with the result of the complete
  // `WHERE` clause, so its values are not visible in the FILTER.
  auto qet =
      h::parseAndPlan(absl::StrCat(prefixes, "SELECT * { ", geoms, "FILTER(",
                                   dist, " <= ?r) } VALUES ?r { 500 }"),
                      ad_utility::testing::getQec(std::string{turtle}));
  std::vector<const SpatialJoin*> spatialJoins;
  collect(*qet, spatialJoins);
  EXPECT_THAT(spatialJoins, IsEmpty());
}

// Run `q` on a small index with the given `turtle` and return the rows of the
// result in TSV format, sorted.
std::vector<std::string> run(const std::string& q,
                             std::string_view kg = turtle) {
  auto* qec = ad_utility::testing::getQec(std::string{kg});
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto parsedQuery =
      SparqlParser::parseQuery(ad_utility::testing::encodedIriManager(), q);
  QueryPlanner qp{qec, cancellationHandle};
  auto qet = qp.createExecutionTree(parsedQuery);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string tsv;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           parsedQuery, *qet, ad_utility::MediaType::tsv, timer,
           cancellationHandle)) {
    tsv += block;
  }
  std::vector<std::string> rows = absl::StrSplit(tsv, '\n');
  ql::ranges::sort(rows);
  return rows;
}

// _____________________________________________________________________________
TEST(QueryPlannerFilterFacts, ResultsAreEqual) {
  auto dist = "geof:distance(?ga, ?gb, unit:M)";
  std::vector<std::pair<std::string, size_t>> queries{
      // Pairs within 500 m: 4 with itself, a1-a2, a2-a3 (in both directions).
      {absl::StrCat(geoms, "FILTER(", dist, " <= 500)"), 8},
      {absl::StrCat(geoms, "BIND(", dist, " AS ?d) FILTER(?d <= 500)"), 8},
      {absl::StrCat(geoms, "VALUES ?r { 500 } FILTER(", dist, " <= ?r)"), 8},
      {absl::StrCat(geoms, "FILTER(500 >= ", dist, ")"), 8},
      {absl::StrCat(geoms, "FILTER(", dist, " < 500)"), 8},
      // Two radii: the `SpatialJoin` uses 1000 m, the FILTER does the rest.
      {absl::StrCat(geoms, "VALUES ?r { 500 1000 } FILTER(", dist, " <= ?r)"),
       8 + 10},
      // The `BIND` is evaluated before `?gb` is bound.
      {absl::StrCat("?a <p> ?ga . BIND(", dist, " AS ?d) ?b <p> ?gb . ",
                    "FILTER(?d <= 500)"),
       0},
      {absl::StrCat(geoms, "VALUES ?r { 500 UNDEF } FILTER(", dist, " <= ?r)"),
       8},
      // `?gb` is never bound, so the `SpatialJoin` can never be completed.
      {absl::StrCat("?a <p> ?ga . FILTER(", dist, " <= 500)"), 0},
      // The FILTER inside the `OPTIONAL` cannot see `?ga`.
      {absl::StrCat("?a <p> ?ga . OPTIONAL { ?b <p> ?gb . BIND(", dist,
                    " AS ?d) FILTER(?d <= 500) }"),
       4},
  };
  for (const auto& [body, numRows] : queries) {
    auto q = query(body);
    auto withFacts = [&q] {
      auto cleanup = factsEnabled(true);
      return run(q);
    }();
    auto withoutFacts = [&q] {
      auto cleanup = factsEnabled(false);
      return run(q);
    }();
    EXPECT_EQ(withFacts, withoutFacts) << body;
    // The header and the empty line after the last row.
    EXPECT_EQ(withFacts.size(), numRows + 2) << body;
  }
}

// Labels with different language tags.
constexpr std::string_view langTurtle =
    "<x1> <label> \"a\"@en . <x1> <label> \"b\"@de . <x2> <label> \"c\"@fr . "
    "<x3> <label> \"d\" . <x3> <other> \"e\"@en .";

// Plan `body` on the index with the labels from above and return the
// predicates of all its `IndexScan`s, and the descriptors of all its
// `Filter`s.
std::pair<std::vector<std::string>, std::vector<std::string>> planLang(
    std::string_view body) {
  auto qet = h::parseAndPlan(
      query(body), ad_utility::testing::getQec(std::string{langTurtle}));
  std::vector<const IndexScan*> scans;
  collect(*qet, scans);
  std::vector<const Filter*> filters;
  collect(*qet, filters);
  std::vector<std::string> predicates;
  for (const auto* scan : scans) {
    predicates.push_back(scan->predicate().toString());
  }
  std::vector<std::string> descriptors;
  for (const auto* filter : filters) {
    descriptors.push_back(filter->getDescriptor());
  }
  return {predicates, descriptors};
}

// _____________________________________________________________________________
TEST(QueryPlannerFilterFacts, LanguageFilters) {
  using ::testing::HasSubstr;
  using ::testing::UnorderedElementsAre;
  std::string_view direct = "?x <label> ?y FILTER(LANG(?y) = \"en\")";
  std::string_view bind =
      "?x <label> ?y BIND(LANG(?y) AS ?l) FILTER(?l = \"en\")";
  std::string_view in =
      "?x <label> ?y BIND(LANG(?y) AS ?l) FILTER(?l IN (\"en\", \"de\"))";
  {
    // The triple is rewritten, and the FILTER is kept.
    auto cleanup = factsEnabled(true);
    for (auto body : {direct, bind}) {
      auto [predicates, filters] = planLang(body);
      EXPECT_THAT(predicates, ElementsAre("@en@<label>")) << body;
      EXPECT_THAT(filters, ElementsAre(HasSubstr("\"en\""))) << body;
    }
    auto [predicates, filters] = planLang(in);
    EXPECT_THAT(predicates, UnorderedElementsAre("@en@<label>", "@de@<label>"));
    EXPECT_THAT(filters, ElementsAre(HasSubstr("IN")));
  }
  // Without the filter fact recognition, only the direct shape is rewritten
  // (by the parser, which removes the FILTER).
  auto cleanup = factsEnabled(false);
  auto [predicates, filters] = planLang(direct);
  EXPECT_THAT(predicates, ElementsAre("@en@<label>"));
  EXPECT_THAT(filters, IsEmpty());
  std::tie(predicates, filters) = planLang(bind);
  EXPECT_THAT(predicates, ElementsAre("<label>"));
  EXPECT_THAT(filters, ElementsAre(HasSubstr("?l = \"en\"")));
}

// _____________________________________________________________________________
TEST(QueryPlannerFilterFacts, LanguageFilterResultsAreEqual) {
  std::vector<std::pair<std::string, size_t>> queries{
      {"?x <label> ?y FILTER(LANG(?y) = \"en\")", 1},
      {"?x <label> ?y BIND(LANG(?y) AS ?l) FILTER(?l = \"en\")", 1},
      {"?x <label> ?y BIND(LANG(?y) AS ?l) FILTER(?l IN (\"en\", \"de\"))", 2},
      {"?x <label> ?y FILTER(LANG(?y) = \"en\" || LANG(?y) = \"fr\")", 2},
      {"?x <label> ?y VALUES ?l { \"en\" \"de\" } FILTER(LANG(?y) = ?l)", 2},
      {"?x ?p ?y BIND(LANG(?y) AS ?l) FILTER(?l = \"en\")", 2},
      {"?x <label> ?y BIND(LANG(?y) AS ?l) FILTER(?l = \"\")", 1},
  };
  for (const auto& [body, numRows] : queries) {
    auto q = query(body);
    auto withFacts = [&q] {
      auto cleanup = factsEnabled(true);
      return run(q, langTurtle);
    }();
    auto withoutFacts = [&q] {
      auto cleanup = factsEnabled(false);
      return run(q, langTurtle);
    }();
    EXPECT_EQ(withFacts, withoutFacts) << body;
    EXPECT_EQ(withFacts.size(), numRows + 2) << body;
  }
}

}  // namespace
