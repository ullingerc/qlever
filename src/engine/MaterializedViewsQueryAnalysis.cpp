// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/MaterializedViewsQueryAnalysis.h"

#include <optional>
#include <variant>

#include "engine/IndexScan.h"
#include "engine/MaterializedViews.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"
#include "rdfTypes/Iri.h"

namespace materializedViewsQueryAnalysis {

// _____________________________________________________________________________
ad_utility::HashSet<Variable> getVariablesPresentInBasicGraphPatterns(
    const std::vector<parsedQuery::GraphPatternOperation>& graphPatterns) {
  ad_utility::HashSet<Variable> vars;
  for (const auto& graphPattern : graphPatterns) {
    if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
      continue;
    }
    for (const auto& triple : graphPattern.getBasic()._triples) {
      if (triple.s_.isVariable()) {
        vars.insert(triple.s_.getVariable());
      }
      if (triple.o_.isVariable()) {
        vars.insert(triple.o_.getVariable());
      }
      if (auto p = triple.getPredicateVariable()) {
        vars.insert(p.value());
      }
    }
  }
  return vars;
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Optional&) const {
  // TODO<ullingerc> Analyze if the optional binds values from the outside
  // query.
  return false;
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Bind& bind) const {
  return !variables_.contains(bind._target);
}

// _____________________________________________________________________________
bool BasicGraphPatternsInvariantTo::operator()(
    const parsedQuery::Values& values) const {
  return !std::ranges::any_of(
      values._inlineValues._variables,
      [this](const auto& var) { return variables_.contains(var); });
}

// _____________________________________________________________________________
std::optional<UserQueryChain> QueryPatternCache::checkSimpleChain(
    std::shared_ptr<IndexScan> left, std::shared_ptr<IndexScan> right) const {
  if (!left || !right || !left->predicate().isIri() ||
      !right->predicate().isIri()) {
    return std::nullopt;
  }
  if (left->object() == right->subject() &&
      left->subject() != right->object() && left->subject() != left->object() &&
      right->subject() != right->object() && left->object().isVariable() &&
      right->object().isVariable()) {
    materializedViewsQueryAnalysis::ChainedPredicates preds{
        left->predicate().getIri().toStringRepresentation(),
        right->predicate().getIri().toStringRepresentation()};
    if (simpleChainCache_.contains(preds)) {
      return UserQueryChain{left->subject(), left->object().getVariable(),
                            right->object().getVariable(),
                            simpleChainCache_.at(preds)};
    }
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<UserQueryStar> QueryPatternCache::checkStar(
    const BasicGraphPattern& triples) const {
  ad_utility::HashMap<Variable, ad_utility::HashSet<Variable>> starUsedObjects;
  ad_utility::HashMap<
      Variable,
      ad_utility::HashMap<ad_utility::triple_component::Iri, Variable>>
      starPredicates;

  for (const auto& triple : triples._triples) {
    if (!triple.getSimplePredicate().has_value()) {
      continue;
    }
    const auto simpleTriple = triple.getSimple();
    if (!simpleTriple.s_.isVariable() || !simpleTriple.p_.isIri() ||
        !simpleTriple.o_.isVariable()) {
      // TODO fixed object?
      continue;
    }
    const auto& s = simpleTriple.s_.getVariable();
    const auto& p = simpleTriple.s_.getIri();
    const auto& o = simpleTriple.s_.getVariable();
    if (s == o) {
      continue;
    }
    if (!starPredicates.contains(s)) {
      starPredicates[s] = {};
      starUsedObjects[s] = {};
    } else if (starPredicates.at(s).contains(p) ||
               starUsedObjects.at(s).contains(o)) {
      // This triple would add a connection between arms of a star. Ignore it -
      // it needs to be joined outside of the materialized view scan.
      continue;
    }
    starPredicates.at(s).insert({p, o});
    starUsedObjects.at(s).insert(o);
  }
  // TODO check against actual views
  // TODO optional,
  return std::nullopt;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeSimpleChain(ViewPtr view, const SparqlTriple& a,
                                           const SparqlTriple& b) {
  // Check predicates.
  auto aPred = a.getSimplePredicate();
  if (!aPred.has_value()) {
    return false;
  }
  auto bPred = b.getSimplePredicate();
  if (!bPred.has_value()) {
    return false;
  }

  // Check variables.
  if (!a.s_.isVariable()) {
    return false;
  }
  auto aSubj = a.s_.getVariable();

  if (!a.o_.isVariable() || a.o_.getVariable() == aSubj) {
    return false;
  }
  auto chainVar = a.o_.getVariable();

  if (!b.s_.isVariable() || b.s_.getVariable() != chainVar) {
    return false;
  }

  if (!b.o_.isVariable() || b.o_.getVariable() == chainVar ||
      b.o_.getVariable() == aSubj) {
    return false;
  }
  auto bObj = b.o_.getVariable();

  // Insert chain to cache.
  ChainedPredicates preds{aPred.value(), bPred.value()};
  if (!simpleChainCache_.contains(preds)) {
    simpleChainCache_[preds] = std::make_shared<std::vector<ChainInfo>>();
  }
  simpleChainCache_[preds]->push_back(
      ChainInfo{std::move(aSubj), std::move(chainVar), std::move(bObj), view});
  return true;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeStar(ViewPtr view,
                                    const std::vector<SparqlTriple>& triples) {
  // All triples must have the same subject, a variable.
  if (triples.size() < 2 || !triples.at(0).s_.isVariable()) {
    return false;
  }
  const Variable& subject = triples.at(0).s_.getVariable();
  ad_utility::HashSet<Variable> usedObjects;
  using VarAndIsOptional = std::pair<Variable, bool>;
  ad_utility::HashMap<ad_utility::triple_component::Iri, VarAndIsOptional>
      predicates;

  for (const auto& triple : triples) {
    if (!triple.s_.isVariable() || triple.s_.getVariable() != subject ||
        !triple.o_.isVariable() || !triple.getSimplePredicate().has_value() ||
        usedObjects.contains(triple.o_.getVariable())) {
      return false;
    }
    const auto simpleTriple = triple.getSimple();
    if (!simpleTriple.p_.isIri()) {
      return false;
    }
    const auto& o = triple.o_.getVariable();
    const auto& p = simpleTriple.p_.getIri();
    usedObjects.insert(o);
    predicates.insert({p, {o, false}});
  }

  // TODO data structure
  // TODO optionals containing single triple (and without coalesce behavior)
  return false;
}

// _____________________________________________________________________________
bool QueryPatternCache::analyzeView(ViewPtr view) {
  const auto& query = view->originalQuery();
  if (!query.has_value()) {
    return false;
  }

  // We do not need the `EncodedIriManager` because we are only interested in
  // analyzing the query structure, not in converting its components to
  // `ValueId`s.
  EncodedIriManager e;
  auto parsed = SparqlParser::parseQuery(&e, query.value(), {});

  // TODO<ullingerc> Do we want to report the reason for non-optimizable
  // queries?

  const auto& graphPatterns = parsed._rootGraphPattern._graphPatterns;
  BasicGraphPatternsInvariantTo invariantCheck{
      getVariablesPresentInBasicGraphPatterns(graphPatterns)};
  // Filter out graph patterns that do not change the result of the basic graph
  // pattern analyzed.
  // TODO<ullingerc> Deduplication necessary when reading, the variables should
  // not be in the first three
  auto graphPatternsFiltered =
      ::ranges::to<std::vector>(parsed._rootGraphPattern._graphPatterns |
                                ql::views::filter([&](const auto& pattern) {
                                  return !std::visit(invariantCheck, pattern);
                                }));
  if (graphPatternsFiltered.size() != 1) {
    return false;
  }
  const auto& graphPattern = graphPatternsFiltered.at(0);
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(graphPattern)) {
    return false;
  }
  // TODO<ullingerc> Property path is stored as a single predicate here.
  const auto& triples = graphPattern.getBasic()._triples;
  if (triples.size() == 0) {
    return false;
  }
  bool patternFound = false;

  // TODO<ullingerc> Possibly handle chain by property path.
  if (triples.size() == 2) {
    const auto& a = triples.at(0);
    const auto& b = triples.at(1);
    if (!analyzeSimpleChain(view, a, b)) {
      patternFound = patternFound || analyzeSimpleChain(view, b, a);
    } else {
      patternFound = true;
    }
  }

  patternFound = patternFound || analyzeStar(view, triples);

  // Remember predicates that appear in certain views, only if any pattern is
  // detected.
  if (patternFound) {
    for (const auto& triple : triples) {
      auto predicate = triple.getSimplePredicate();
      if (predicate.has_value()) {
        if (!predicateInView_.contains(predicate.value())) {
          predicateInView_[predicate.value()] = {};
        }
        predicateInView_[predicate.value()].push_back(view);
      }
    }
  }

  return patternFound;
}

}  // namespace materializedViewsQueryAnalysis
