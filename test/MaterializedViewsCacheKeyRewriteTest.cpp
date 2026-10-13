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
#include "./util/IdTableHelpers.h"
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
    auto trace = generateLocationTrace(sourceLocation);
    EXPECT_EQ(getQueryResultAsIdTable(query).numRows(), expectedRows);
    qlv().writeMaterializedView(name, query);
    // The view is always detected if the user query is exactly the view query.
    qpExpect<false>(qlv(), query, qpMatcher);
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
  qpExpect<false>(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m .
        ?o2 ^<p2> ?m .
      }
    )",
                  viewScan("testView1", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the view query only with a property path instead of a manual
  // join.
  qpExpect<false>(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3>/<p2> ?o2 .
      }
    )",
                  viewScan("testView1", "?s", "?_QLever_internal_variable_qp_0",
                           "?o1", 4, {{3, V{"?o2"}}}));

  // User query contains an additional filter.
  qpExpect<false>(
      qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3>/<p2> ?o2 .
        FILTER (?o2 > 5)
      }
    )",
      h::Filter("?o2 > 5",
                viewScan("testView1", "?s", "?_QLever_internal_variable_qp_0",
                         "?o1", 4, {{3, V{"?o2"}}})));

  // User query contains an additional join.
  qpExpect<false>(qlv(), R"(
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
  qpExpect<false>(
      qlv(), optionalQuery,
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
  qpExpect<false>(qlv(), R"(
        SELECT ?s ?m ?o1 ?o2 {
          ?s <p1> ?o1 .
          ?s <p3> ?m .
          ?m <p2> ?o2 .
          FILTER (?o2 < 6)
          FILTER (?m != <s1>)
        }
      )",
                  h::Filter("?m != <s1>", viewScan("testView2", "?s", "?m",
                                                   "?o1", 4, {{3, V{"?o2"}}})));

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
  qpExpect<false>(qlv(), writeQuery1,
                  viewScan("testView4", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the same as the write query but it contains an additional
  // `BIND`.
  qpExpect<false>(qlv(), R"(
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
TEST_F(MaterializedViewsCacheKeyRewriteTest, isFunctional) {
  using materializedViewsQueryAnalysis::isFunctional;
  auto qec = getQec();
  auto check = [&qec](std::string predicate, bool inputIsSubject) {
    return isFunctional(
        *qec, {V{"?in"}, std::move(predicate), V{"?out"}, inputIsSubject});
  };
  // `<s1>` has three objects for `<p3>`, but each object has one subject.
  EXPECT_FALSE(check("<p3>", true));
  EXPECT_TRUE(check("<p3>", false));
  EXPECT_TRUE(check("<p4>", true));
  // `<s3>` has two objects for `<p2>`.
  EXPECT_FALSE(check("<p2>", true));
  // Unknown and QLever-internal predicates.
  EXPECT_FALSE(check("<doesNotExist>", true));
  EXPECT_FALSE(check(std::string{HAS_PREDICATE_PREDICATE}, true));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsCacheKeyRewriteTest, FunctionalOptionalRewrite) {
  // Compare the result of `query` with the result without query rewriting.
  auto expectSameResult = [this](const std::string& query,
                                 source_location sourceLocation =
                                     AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(sourceLocation);
    auto actual = getQueryResultAsIdTable(query);
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::enableMaterializedViewQueryRewrite_>(false);
    qlv().clearQueryResultCache();
    EXPECT_THAT(actual, matchesIdTable(getQueryResultAsIdTable(query)));
  };

  // `<p4>` is functional, so the `OPTIONAL` only adds the column `?x`.
  const std::string viewQuery = R"(
    SELECT ?s ?m ?o1 ?x {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      OPTIONAL { ?m <p4> ?x }
    }
  )";
  EXPECT_EQ(getQueryResultAsIdTable(viewQuery).numRows(), 5);
  qlv().writeMaterializedView("optView", viewQuery);
  qpExpect<false>(qlv(), viewQuery,
                  viewScan("optView", "?s", "?m", "?o1", 4, {{3, V{"?x"}}}));

  // The view can be used for a query without the `OPTIONAL`.
  const std::string withoutOptional = R"(
    SELECT * { ?s <p1> ?o1 . ?s <p3> ?m . }
  )";
  qpExpect<false>(qlv(), withoutOptional,
                  viewScan("optView", "?s", "?m", "?o1", 3));
  expectSameResult(withoutOptional);

  // The `OPTIONAL` is pushed down into the view through a `Join`.
  const std::string throughJoin = R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
      ?s view:optView-m ?m . ?s <p1> ?o1 .
      OPTIONAL { ?m <p4> ?y }
    }
  )";
  qpExpect(qlv(), throughJoin,
           h::Join(h::IndexScanFromStrings("?s", "<p1>", "?o1"),
                   viewScan("optView", "?s", "?m", "?_ql_materialized_view_o",
                            3, {{3, V{"?y"}}})));
  expectSameResult(throughJoin);

  // The `OPTIONAL` is pushed down into an explicit scan of the view.
  const std::string explicitScan = R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
      ?s view:optView-m ?m .
      OPTIONAL { ?m <p4> ?y }
    }
  )";
  qpExpect(qlv(), explicitScan,
           viewScan("optView", "?s", "?m", "?_ql_materialized_view_o", 3,
                    {{3, V{"?y"}}}));
  expectSameResult(explicitScan);

  // The view only contains the `OPTIONAL` with the input `?m`.
  const std::string otherInput = R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
      ?s view:optView-m ?m .
      OPTIONAL { ?s <p4> ?y }
    }
  )";
  qpExpect(qlv(), otherInput,
           h::OptionalJoin(::testing::A<const QueryExecutionTree&>(),
                           h::IndexScanFromStrings("?s", "<p4>", "?y")));
  expectSameResult(otherInput);

  // `<p3>` is not functional, so the `OPTIONAL` adds rows.
  const std::string nonFunctionalQuery = R"(
    SELECT ?s ?o1 ?m {
      ?s <p1> ?o1 .
      OPTIONAL { ?s <p3> ?m }
    }
  )";
  EXPECT_EQ(getQueryResultAsIdTable(nonFunctionalQuery).numRows(), 5);
  qlv().writeMaterializedView("nonFunctionalView", nonFunctionalQuery);

  // The view can't be used for a query without the `OPTIONAL`.
  const std::string singleTriple = "SELECT * { ?s <p1> ?o1 }";
  EXPECT_EQ(getQueryResultAsIdTable(singleTriple).numRows(), 2);
  expectSameResult(singleTriple);

  // The `OPTIONAL` is not pushed down into the view.
  const std::string nonFunctionalPushDown = R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * {
      ?s view:nonFunctionalView-o1 ?o1 .
      OPTIONAL { ?s <p3> ?z }
    }
  )";
  EXPECT_EQ(getQueryResultAsIdTable(nonFunctionalPushDown).numRows(), 13);
  expectSameResult(nonFunctionalPushDown);

  // No push down with a `FROM` clause, the `OPTIONAL` finds no match then.
  const std::string withFrom = R"(
    PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
    SELECT * FROM <doesNotExist> {
      ?s view:optView-m ?m .
      OPTIONAL { ?m <p4> ?y }
    }
  )";
  expectSameResult(withFrom);
}
