// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/functional/bind_front.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./QueryPlannerTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsCacheKeyRewriteTest, CacheKeyRewrite) {
  auto prepareView = [this](const auto& name, const auto& query,
                            size_t expectedRows, const auto& qpMatcher,
                            source_location sourceLocation =
                                AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    EXPECT_EQ(getQueryResultAsIdTable(query).numRows(), expectedRows);
    qlv().writeMaterializedView(name, query);
    qlv().loadMaterializedView(name);
    // The view is always detected if the user query is exactly the view query.
    qpExpect(qlv(), query, qpMatcher);
  };

  // Neither a star nor a chain.
  const std::string writeQuery1 = R"(
    SELECT ?s ?m ?o1 ?o2 {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
    }
  )";
  prepareView("testView1", writeQuery1, 2,
              viewScan("testView1", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the view query only with an edge reversed.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m .
        ?o2 ^<p2> ?m .
      }
    )",
           viewScan("testView1", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the view query only with a property path instead of a manual
  // join.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3>/<p2> ?o2 .
      }
    )",
           viewScan("testView1", "?s", "?_QLever_internal_variable_qp_0", "?o1",
                    4, {{3, V{"?o2"}}}));

  // User query contains an additional filter.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3>/<p2> ?o2 .
        FILTER (?o2 > 5)
      }
    )",
           h::Filter("?o2 > 5", viewScan("testView1", "?s",
                                         "?_QLever_internal_variable_qp_0",
                                         "?o1", 4, {{3, V{"?o2"}}})));

  // User query contains an additional join.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m1 .
        ?m2 <p4> ?o2 .
        ?m1 <p2> ?m2 .
      }
    )",
           h::Join(h::Sort(viewScan("testView1", "?s", "?m1", "?o1", 4,
                                    {{3, V{"?m2"}}})),
                   h::IndexScanFromStrings("?m2", "<p4>", "?o2")));

  // User query contains an additional `OPTIONAL`.
  const std::string optionalQuery = R"(
      SELECT ?s ?o1 ?m1 ?m2 ?o2 {
        ?s <p1> ?o1 .
        ?s <p3> ?m1 .
        ?m1 <p2> ?m2 .
        OPTIONAL { ?m2 <p4> ?o2 }
      }
    )";
  qpExpect(qlv(), optionalQuery,
           h::OptionalJoin(h::Sort(viewScan("testView1", "?s", "?m1", "?o1", 4,
                                            {{3, V{"?m2"}}})),
                           h::IndexScanFromStrings("?m2", "<p4>", "?o2")));

  // Filter in write query.
  qlv().unloadMaterializedView("testView1");
  const std::string writeQuery2 = R"(
    SELECT ?s ?m ?o1 ?o2 {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      FILTER (?o2 < 6)
    }
  )";
  prepareView("testView2", writeQuery2, 1,
              viewScan("testView2", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // Filter in write query and an additional filter in the user query.
  qpExpect(qlv(), R"(
        SELECT ?s ?m ?o1 ?o2 {
          ?s <p1> ?o1 .
          ?s <p3> ?m .
          ?m <p2> ?o2 .
          FILTER (?o2 < 6)
          FILTER (?m != <s1>)
        }
      )",
           h::Filter("?m != <s1>", viewScan("testView2", "?s", "?m", "?o1", 4,
                                            {{3, V{"?o2"}}})));

  // Optional in write query.
  qlv().unloadMaterializedView("testView2");
  prepareView("testView3", optionalQuery, 2,
              viewScan("testView3", "?s", "?o1", "?m1", 5,
                       {{3, V{"?m2"}}, {4, V{"?o2"}}}));

  // Write query containing a `BIND`.
  const std::string bindQuery = R"(
    SELECT ?s ?m ?o1 ?o2 ?b {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      BIND (5 * ?o2 AS ?b)
    }
  )";
  qlv().unloadMaterializedView("testView3");
  prepareView("testView4", bindQuery, 2,
              viewScan("testView4", "?s", "?m", "?o1", 5,
                       {{3, V{"?o2"}}, {4, V{"?b"}}}));

  // User query is the same as the write query but the `BIND` is omitted.
  qpExpect(qlv(), writeQuery1,
           viewScan("testView4", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the same as the write query but it contains an additional
  // `BIND`.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m .
        ?m <p2> ?o2 .
        BIND (5 * ?o2 AS ?b1)
        BIND (?o2 + 2 AS ?b2)
      }
    )",
           h::Bind(viewScan("testView4", "?s", "?m", "?o1", 5,
                            {{3, V{"?o2"}}, {4, V{"?b1"}}}),
                   "?o2 + 2", V{"?b2"}));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsFixedFirstColumnTest, FixedFirstColumn) {
  const std::string writeQuery = R"(
    SELECT ?s ?m ?o {
      ?s <p1> ?m .
      ?m <p2> ?o .
    }
  )";
  qlv().writeMaterializedView("chainView", writeQuery);
  qlv().loadMaterializedView("chainView");
  qpExpect(qlv(), writeQuery, viewScanSimple("chainView", "?s", "?m", "?o"));

  // Helper that checks that the query is answered by a scan on the view with
  // the first column fixed and that this yields the same result as without the
  // view.
  auto expectFixedFirstColumnScan = [this](const std::string& query,
                                           const std::string& fixedValue,
                                           source_location sourceLocation =
                                               AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    qpExpect(qlv(), query, viewScanSimple("chainView", fixedValue, "?m", "?o"));
    qlv().clearQueryResultCache();
    auto withView = getQueryResultAsIdTable(query);
    qlv().unloadMaterializedView("chainView");
    qlv().clearQueryResultCache();
    auto withoutView = getQueryResultAsIdTable(query);
    qlv().loadMaterializedView("chainView");
    EXPECT_EQ(withView, withoutView);
  };

  // The user query is the view query with the first column fixed. Note that
  // neither of the two values below is one of the values the view's cache key
  // template was computed with (those are the smallest and the largest value of
  // the first column, `<a1>` and `<a4>`).
  expectFixedFirstColumnScan(R"(SELECT ?m ?o { <a2> <p1> ?m . ?m <p2> ?o })",
                             "<a2>");
  expectFixedFirstColumnScan(R"(SELECT ?m ?o { <a3> <p1> ?m . ?m <p2> ?o })",
                             "<a3>");

  // The same for a value that does not occur in the first column at all.
  expectFixedFirstColumnScan(R"(SELECT ?m ?o { <b1> <p1> ?m . ?m <p2> ?o })",
                             "<b1>");

  // A fixed value in a column other than the first one is not rewritten.
  qpExpect(qlv(), R"(SELECT ?s ?m { ?s <p1> ?m . ?m <p2> <c2> })",
           h::Join(h::IndexScanFromStrings("?s", "<p1>", "?m"),
                   h::IndexScanFromStrings("?m", "<p2>", "<c2>")));

  // A query that is not the view's query is not rewritten, even though it has a
  // fixed first column.
  qpExpect(qlv(), R"(SELECT ?m ?o { <a2> <p1> ?m . ?m <p4> ?o })",
           h::Join(h::IndexScanFromStrings("<a2>", "<p1>", "?m"),
                   h::IndexScanFromStrings("?m", "<p4>", "?o")));
}
