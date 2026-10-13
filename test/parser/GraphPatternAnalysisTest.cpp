// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "index/vocabulary/EncodedIriManager.h"
#include "parser/GraphPatternAnalysis.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"
#include "parser/SparqlTriple.h"
#include "parser/VariableCounter.h"

using namespace graphPatternAnalysis;
using parsedQuery::VariableCounter;
using V = Variable;

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, Bind) {
  VariableCounter counter;
  counter(std::vector{V{"?x"}, V{"?x"}, V{"?y"}});
  BasicGraphPatternsInvariantTo invariantTo{counter};

  // Test that BIND is invariant when its target variable is not in our set.
  parsedQuery::Bind bind1{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?a"}),
      Variable{"?z"}};
  EXPECT_TRUE(invariantTo(bind1));

  // Test that BIND is not invariant when its target variable is in our set.
  parsedQuery::Bind bind2{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?a"}),
      Variable{"?x"}};

  EXPECT_FALSE(invariantTo(bind2));

  // Test that BIND is invariant when we have no variables to check.
  BasicGraphPatternsInvariantTo invariantTo2{VariableCounter{}};
  parsedQuery::Bind bind{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?a"}),
      Variable{"?x"}};
  EXPECT_TRUE(invariantTo2(bind));
}

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, Values) {
  VariableCounter counter;
  counter(std::vector{V{"?x"}, V{"?x"}, V{"?y"}});
  BasicGraphPatternsInvariantTo invariantTo{counter};

  // Test VALUES with exactly one row and no variable overlap.
  parsedQuery::Values values;
  values._inlineValues._variables = {Variable{"?a"}, Variable{"?b"}};
  values._inlineValues._values = {
      {TripleComponent::Iri::fromIriref("<value1>"),
       TripleComponent::Iri::fromIriref("<value2>")}};
  EXPECT_TRUE(invariantTo(values));

  // Test VALUES with one row but with variable overlap.
  parsedQuery::Values values2;
  values2._inlineValues._variables = {Variable{"?x"}, Variable{"?b"}};
  values2._inlineValues._values = {
      {TripleComponent::Iri::fromIriref("<value1>"),
       TripleComponent::Iri::fromIriref("<value2>")}};
  EXPECT_FALSE(invariantTo(values2));

  // Test VALUES with multiple rows (not invariant even without variable
  // overlap).
  parsedQuery::Values values3;
  values3._inlineValues._variables = {Variable{"?a"}};
  values3._inlineValues._values = {
      {TripleComponent::Iri::fromIriref("<value1>")},
      {TripleComponent::Iri::fromIriref("<value2>")}};
  EXPECT_FALSE(invariantTo(values3));

  // Test VALUES with zero rows (not invariant).
  parsedQuery::Values values4;
  values4._inlineValues._variables = {Variable{"?a"}};
  values4._inlineValues._values = {};
  EXPECT_FALSE(invariantTo(values4));
}

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, NotInvariant) {
  // Test the base case: is not invariant.
  VariableCounter counter;
  counter(V{"?x"});
  BasicGraphPatternsInvariantTo invariantTo{counter};

  SparqlTripleSimple example{Variable{"?s"}, Variable{"?p"}, Variable{"?o"}};
  auto triple = SparqlTriple::fromSimple(example);
  parsedQuery::BasicGraphPattern bgp{{triple}};
  EXPECT_FALSE(invariantTo(bgp));

  parsedQuery::Optional optional{parsedQuery::GraphPattern{}};
  EXPECT_FALSE(invariantTo(optional));
}

namespace {
// Parse `query` and return its root graph pattern.
parsedQuery::GraphPattern parseGraphPattern(std::string query) {
  EncodedIriManager encodedIriManager;
  return SparqlParser::parseQuery(&encodedIriManager, std::move(query))
      ._rootGraphPattern;
}

// Return the `ColumnAddingOptional`s of `query` as tuples of their input,
// predicate, output and `inputIsSubject_`.
auto columnAddingOptionals(std::string query) {
  std::vector<std::tuple<V, std::string, V, bool>> result;
  for (const auto& [optional, t] :
       findColumnAddingOptionals(parseGraphPattern(std::move(query)))) {
    EXPECT_NE(optional, nullptr);
    result.emplace_back(t.input_, t.predicate_, t.output_, t.inputIsSubject_);
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(GraphPatternAnalysisTest, findColumnAddingOptionals) {
  using ::testing::ElementsAre;
  using ::testing::IsEmpty;
  using T = std::tuple<V, std::string, V, bool>;

  EXPECT_THAT(
      columnAddingOptionals("SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x } }"),
      ElementsAre(T{V{"?o"}, "<q>", V{"?x"}, true}));
  EXPECT_THAT(
      columnAddingOptionals("SELECT * { ?s <p> ?o OPTIONAL { ?x <q> ?o } }"),
      ElementsAre(T{V{"?o"}, "<q>", V{"?x"}, false}));
  // Several `OPTIONAL`s. The output of the first one is used by the second
  // one, whose input is not bound by a triple.
  EXPECT_THAT(columnAddingOptionals("SELECT * { ?s <p> ?o "
                                    "OPTIONAL { ?s <q> ?x } "
                                    "OPTIONAL { ?x <r> ?y } "
                                    "OPTIONAL { ?o <r> ?z } }"),
              ElementsAre(T{V{"?o"}, "<r>", V{"?z"}, true}));

  // Not column-adding.
  for (std::string query : {
           // The input is bound only after the `OPTIONAL`.
           "SELECT * { OPTIONAL { ?o <q> ?x } ?s <p> ?o }",
           // The input is not bound by a triple.
           "SELECT * { BIND(1 AS ?o) OPTIONAL { ?o <q> ?x } }",
           // The output is used elsewhere.
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x } ?x <r> ?y }",
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x } FILTER(?x = 1) }",
           // Both or none of the variables are bound.
           "SELECT * { ?s <p> ?o OPTIONAL { ?s <q> ?o } }",
           "SELECT * { ?s <p> ?o OPTIONAL { ?x <q> ?y } }",
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?o } }",
           // No variable on one side.
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> <x> } }",
           // No simple predicate.
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q>+ ?x } }",
           "SELECT * { ?s <p> ?o OPTIONAL { ?o ?q ?x } }",
           // Not a single triple.
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x . ?x <r> ?y } }",
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x FILTER(?x = 1) } }",
           "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x BIND(1 AS ?y) } }",
       }) {
    EXPECT_THAT(columnAddingOptionals(query), IsEmpty()) << query;
  }
}

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, Optional) {
  auto gp = parseGraphPattern(
      "SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?x } OPTIONAL { ?o <r> ?y } }");
  const auto& q = std::get<parsedQuery::Optional>(gp._graphPatterns.at(1));
  const auto& r = std::get<parsedQuery::Optional>(gp._graphPatterns.at(2));

  // Without `isFunctional`, no `OPTIONAL` is invariant.
  BasicGraphPatternsInvariantTo noFunctional{gp};
  EXPECT_FALSE(noFunctional(q));
  EXPECT_FALSE(noFunctional(r));

  // Only the `OPTIONAL` with a functional predicate is invariant.
  BasicGraphPatternsInvariantTo onlyQ{
      gp, [](const SingleTripleOptional& t) { return t.predicate_ == "<q>"; }};
  EXPECT_TRUE(onlyQ(q));
  EXPECT_FALSE(onlyQ(r));

  // An `OPTIONAL` that is not column-adding is never invariant.
  auto gp2 = parseGraphPattern("SELECT * { ?s <p> ?o OPTIONAL { ?o <q> ?s } }");
  BasicGraphPatternsInvariantTo all{
      gp2, [](const SingleTripleOptional&) { return true; }};
  EXPECT_FALSE(all(std::get<parsedQuery::Optional>(gp2._graphPatterns.at(1))));
}
