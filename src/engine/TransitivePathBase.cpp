// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include "TransitivePathBase.h"

#include <absl/strings/str_cat.h>

#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/Distinct.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/TransitivePathBinSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "global/RuntimeParameters.h"
#include "util/Exception.h"

// _____________________________________________________________________________
TransitivePathBase::TransitivePathBase(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, Graphs activeGraphs)
    : Operation(qec),
      subtree_(std::move(child)),
      lhs_(std::move(leftSide)),
      rhs_(std::move(rightSide)),
      minDist_(minDist),
      maxDist_(maxDist),
      activeGraphs_{std::move(activeGraphs)} {
  AD_CORRECTNESS_CHECK(qec != nullptr);
  AD_CORRECTNESS_CHECK(subtree_);
  if (lhs_.isVariable()) {
    variableColumns_[lhs_.value_.getVariable()] = makeAlwaysDefinedColumn(0);
  }
  if (rhs_.isVariable()) {
    variableColumns_[rhs_.value_.getVariable()] = makeAlwaysDefinedColumn(1);
  }
  if (minDist_ == 0) {
    auto& startingSide = decideDirection().first;
    // If we have hardcoded differing values left and right, we can increase the
    // minimum distance to 1. Example: The triple pattern `<x> <p>* <y>` cannot
    // possibly match with length zero because <x> != <y>. Instead we compute
    // `<x> <p>+ <y>` which avoids the performance pessimisation of having to
    // match the iri or literal against the knowledge graph.
    if (!lhs_.isVariable() && !rhs_.isVariable() &&
        lhs_.value_ != rhs_.value_) {
      minDist_ = 1;
    } else if (lhs_.isUnboundVariable() && rhs_.isUnboundVariable()) {
      boundVariableIsForEmptyPath_ = true;
      lhs_.treeAndCol_.emplace(makeEmptyPathSide(qec, activeGraphs_), 0);
    } else if (!startingSide.isVariable()) {
      startingSide.treeAndCol_.emplace(
          joinWithIndexScan(qec, activeGraphs_, startingSide.value_), 0);
    }
  }

  lhs_.outputCol_ = 0;
  rhs_.outputCol_ = 1;
}

namespace {
auto makeInternalVariable(std::string_view string) {
  return Variable{absl::StrCat("?internal_property_path_variable_", string)};
}
}  // namespace

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::joinWithIndexScan(
    QueryExecutionContext* qec, Graphs activeGraphs,
    const TripleComponent& tripleComponent) {
  // TODO<RobinTF> Once prefiltering is propagated to nested index scans, we can
  // simplify this by calling `makeEmptyPathSide` and merging this tree instead.

  // Dummy variables to get a full scan of the index.
  auto x = makeInternalVariable("x");
  auto y = makeInternalVariable("y");
  auto z = makeInternalVariable("z");

  auto joinWithValues = [qec, &tripleComponent, &x](
                            std::shared_ptr<QueryExecutionTree> executionTree) {
    auto valuesClause = ad_utility::makeExecutionTree<Values>(
        qec, parsedQuery::SparqlValues{{x}, {{tripleComponent}}});
    return ad_utility::makeExecutionTree<Join>(qec, std::move(executionTree),
                                               std::move(valuesClause), 0, 0);
  };
  auto selectXVariable =
      [&x](std::shared_ptr<QueryExecutionTree> executionTree) {
        executionTree->getRootOperation()->setSelectedVariablesForSubquery({x});
        return executionTree;
      };
  auto allValues = ad_utility::makeExecutionTree<Union>(
      qec,
      joinWithValues(selectXVariable(ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::SPO,
          SparqlTripleSimple{TripleComponent{x}, y, TripleComponent{z}},
          activeGraphs))),
      joinWithValues(selectXVariable(ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::OPS,
          SparqlTripleSimple{TripleComponent{z}, y, TripleComponent{x}},
          activeGraphs))));
  return ad_utility::makeExecutionTree<Distinct>(qec, std::move(allValues),
                                                 std::vector<ColumnIndex>{0});
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::makeEmptyPathSide(
    QueryExecutionContext* qec, Graphs activeGraphs,
    std::optional<Variable> variable) {
  // Dummy variables to get a full scan of the index.
  auto x = std::move(variable).value_or(makeInternalVariable("x"));
  auto y = makeInternalVariable("y");
  auto z = makeInternalVariable("z");
  // TODO<RobinTF> Ideally we could tell the `IndexScan` to not materialize ?y
  // and ?z in the first place.
  // We don't need to materialize the extra variables y and z in the union.
  auto selectXVariable =
      [&x](std::shared_ptr<QueryExecutionTree> executionTree) {
        executionTree->getRootOperation()->setSelectedVariablesForSubquery({x});
        return executionTree;
      };
  auto allValues = ad_utility::makeExecutionTree<Union>(
      qec,
      selectXVariable(ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::SPO,
          SparqlTripleSimple{TripleComponent{x}, y, TripleComponent{z}},
          activeGraphs)),
      selectXVariable(ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::OPS,
          SparqlTripleSimple{TripleComponent{z}, y, TripleComponent{x}},
          activeGraphs)));
  return ad_utility::makeExecutionTree<Distinct>(qec, std::move(allValues),
                                                 std::vector<ColumnIndex>{0});
}

// _____________________________________________________________________________
TransitivePathBase::~TransitivePathBase() = default;

// _____________________________________________________________________________
std::pair<TransitivePathSide&, TransitivePathSide&>
TransitivePathBase::decideDirection() {
  if (lhs_.isBoundVariable()) {
    LOG(DEBUG) << "Computing TransitivePath left to right" << std::endl;
    return {lhs_, rhs_};
  } else if (rhs_.isBoundVariable() || !rhs_.isVariable()) {
    LOG(DEBUG) << "Computing TransitivePath right to left" << std::endl;
    return {rhs_, lhs_};
  }
  LOG(DEBUG) << "Computing TransitivePath left to right" << std::endl;
  return {lhs_, rhs_};
}

// _____________________________________________________________________________
Result::Generator TransitivePathBase::fillTableWithHull(
    NodeGenerator hull, size_t startSideCol, size_t targetSideCol,
    bool yieldOnce, size_t inputWidth) const {
  return ad_utility::callFixedSizeVi(
      std::array{inputWidth, getResultWidth()},
      [&](auto INPUT_WIDTH, auto OUTPUT_WIDTH) {
        return fillTableWithHullImpl<INPUT_WIDTH, OUTPUT_WIDTH>(
            std::move(hull), startSideCol, targetSideCol, yieldOnce);
      });
}

// _____________________________________________________________________________
Result::Generator TransitivePathBase::fillTableWithHull(NodeGenerator hull,
                                                        size_t startSideCol,
                                                        size_t targetSideCol,
                                                        bool yieldOnce) const {
  return ad_utility::callFixedSizeVi(getResultWidth(), [&](auto WIDTH) {
    return fillTableWithHullImpl<0, WIDTH>(std::move(hull), startSideCol,
                                           targetSideCol, yieldOnce);
  });
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
Result::Generator TransitivePathBase::fillTableWithHullImpl(
    NodeGenerator hull, size_t startSideCol, size_t targetSideCol,
    bool yieldOnce) const {
  ad_utility::Timer timer{ad_utility::Timer::Stopped};
  size_t outputRow = 0;
  IdTableStatic<OUTPUT_WIDTH> table{getResultWidth(), allocator()};
  LocalVocab mergedVocab{};
  for (auto& [node, linkedNodes, localVocab, idTable, inputRow] : hull) {
    timer.cont();
    // As an optimization nodes without any linked nodes should not get yielded
    // in the first place.
    AD_CONTRACT_CHECK(!linkedNodes.empty());
    if (!yieldOnce) {
      table.reserve(linkedNodes.size());
    }
    std::optional<IdTableView<INPUT_WIDTH>> inputView = std::nullopt;
    if (idTable.has_value()) {
      inputView = idTable->template asStaticView<INPUT_WIDTH>();
    }
    for (Id linkedNode : linkedNodes) {
      table.emplace_back();
      table(outputRow, startSideCol) = node;
      table(outputRow, targetSideCol) = linkedNode;

      if (inputView.has_value()) {
        copyColumns<INPUT_WIDTH, OUTPUT_WIDTH>(inputView.value(), table,
                                               inputRow, outputRow);
      }

      outputRow++;
    }

    if (yieldOnce) {
      mergedVocab.mergeWith(localVocab);
    } else {
      timer.stop();
      runtimeInfo().addDetail("IdTable fill time", timer.msecs());
      co_yield {std::move(table).toDynamic(), std::move(localVocab)};
      table = IdTableStatic<OUTPUT_WIDTH>{getResultWidth(), allocator()};
      outputRow = 0;
    }
    timer.stop();
  }
  if (yieldOnce) {
    timer.start();
    runtimeInfo().addDetail("IdTable fill time", timer.msecs());
    co_yield {std::move(table).toDynamic(), std::move(mergedVocab)};
  }
}

// _____________________________________________________________________________
std::string TransitivePathBase::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "TRANSITIVE PATH ";
  if (lhs_.isVariable() && lhs_.value_ == rhs_.value_) {
    // Use a different cache key if the same variable is used left and right,
    // because that changes the behaviour of this operation and variable names
    // are not found in the children's cache keys.
    os << "symmetric ";
  }
  os << "minDist " << minDist_ << " maxDist " << maxDist_ << "\n";

  os << "Left side:\n";
  os << lhs_.getCacheKey();

  os << "Right side:\n";
  os << rhs_.getCacheKey();

  os << "Subtree:\n" << subtree_->getCacheKey() << '\n';

  return std::move(os).str();
}

// _____________________________________________________________________________
std::string TransitivePathBase::getDescriptor() const {
  std::ostringstream os;
  os << "TransitivePath ";
  // If not full transitive hull, show interval as [min, max].
  if (minDist_ > 1 || maxDist_ < std::numeric_limits<size_t>::max()) {
    os << "[" << minDist_ << ", " << maxDist_ << "] ";
  }
  // Left variable or entity name.
  os << lhs_.value_;
  // The predicate.
  auto scanOperation =
      std::dynamic_pointer_cast<IndexScan>(subtree_->getRootOperation());
  if (scanOperation != nullptr) {
    os << " " << scanOperation->predicate() << " ";
  } else {
    // Escaped the question marks to avoid a warning about ignored trigraphs.
    os << R"( <???> )";
  }
  // Right variable or entity name.
  os << rhs_.value_;
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t TransitivePathBase::getResultWidth() const { return resultWidth_; }

// _____________________________________________________________________________
std::vector<ColumnIndex> TransitivePathBase::resultSortedOn() const {
  if (lhs_.isSortedOnInputCol()) {
    return {0};
  }
  if (rhs_.isSortedOnInputCol()) {
    return {1};
  }

  return {};
}

// _____________________________________________________________________________
VariableToColumnMap TransitivePathBase::computeVariableToColumnMap() const {
  return variableColumns_;
}

// _____________________________________________________________________________
bool TransitivePathBase::knownEmptyResult() {
  auto sideHasKnownEmptyResult = [this]() {
    auto tree = decideDirection().first.treeAndCol_;
    return tree.has_value() && tree.value().first->knownEmptyResult();
  };
  return (subtree_->knownEmptyResult() && minDist_ > 0) ||
         sideHasKnownEmptyResult();
}

// _____________________________________________________________________________
float TransitivePathBase::getMultiplicity(size_t col) {
  (void)col;
  // The multiplicities are not known.
  return 1;
}

// _____________________________________________________________________________
uint64_t TransitivePathBase::getSizeEstimateBeforeLimit() {
  if (!lhs_.isVariable() || !rhs_.isVariable()) {
    // If the subject or object is fixed, assume that the number of matching
    // triples is 1000. This will usually be an overestimate, but it will do the
    // job of avoiding query plans that first generate large intermediate
    // results and only then merge them with a triple such as this. In the
    // lhs_.isVar && rhs_.isVar case below, we assume a worst-case blowup of
    // 10000; see the comment there.
    return 1000;
  }
  if (lhs_.treeAndCol_.has_value()) {
    return lhs_.treeAndCol_.value().first->getSizeEstimate();
  }
  if (rhs_.treeAndCol_.has_value()) {
    return rhs_.treeAndCol_.value().first->getSizeEstimate();
  }
  // Set costs to something very large, so that we never compute the complete
  // transitive hull (unless the variables on both sides are not bound in any
  // other way, so that the only possible query plan is to compute the complete
  // transitive hull).
  //
  // NOTE: _subtree->getSizeEstimateBeforeLimit() is the number of triples of
  // the predicate, for which the transitive hull operator (+) is specified. On
  // Wikidata, the predicate with the largest blowup when taking the
  // transitive hull is wdt:P2789 (connects with). The blowup is then from 90K
  // (without +) to 110M (with +), so about 1000 times larger.
  AD_CORRECTNESS_CHECK(lhs_.isVariable() && rhs_.isVariable());
  return subtree_->getSizeEstimate() * 10000;
}

// _____________________________________________________________________________
size_t TransitivePathBase::getCostEstimate() {
  // We assume that the cost of computing the transitive path is proportional to
  // the result size.
  auto costEstimate = getSizeEstimateBeforeLimit();
  // Add the cost for the index scan of the predicate involved.
  for (auto* ptr : getChildren()) {
    if (ptr) {
      costEstimate += ptr->getCostEstimate();
    }
  }
  return costEstimate;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::makeTransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, Graphs activeGraphs) {
  bool useBinSearch =
      RuntimeParameters().get<"use-binsearch-transitive-path">();
  return makeTransitivePath(qec, std::move(child), std::move(leftSide),
                            std::move(rightSide), minDist, maxDist,
                            useBinSearch, std::move(activeGraphs));
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::makeTransitivePath(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> child,
    TransitivePathSide leftSide, TransitivePathSide rightSide, size_t minDist,
    size_t maxDist, bool useBinSearch, Graphs activeGraphs) {
  if (useBinSearch) {
    return std::make_shared<TransitivePathBinSearch>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist, std::move(activeGraphs));
  } else {
    return std::make_shared<TransitivePathHashMap>(
        qec, std::move(child), std::move(leftSide), std::move(rightSide),
        minDist, maxDist, std::move(activeGraphs));
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> TransitivePathBase::getChildren() {
  std::vector<QueryExecutionTree*> res;
  auto addChildren = [](std::vector<QueryExecutionTree*>& res,
                        TransitivePathSide side) {
    if (side.treeAndCol_.has_value()) {
      res.push_back(side.treeAndCol_.value().first.get());
    }
  };
  addChildren(res, lhs_);
  addChildren(res, rhs_);
  res.push_back(subtree_.get());
  return res;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindLeftSide(
    std::shared_ptr<QueryExecutionTree> leftop, size_t inputCol) const {
  return bindLeftOrRightSide(std::move(leftop), inputCol, true);
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindRightSide(
    std::shared_ptr<QueryExecutionTree> rightop, size_t inputCol) const {
  return bindLeftOrRightSide(std::move(rightop), inputCol, false);
}

// _____________________________________________________________________________
std::shared_ptr<QueryExecutionTree> TransitivePathBase::matchWithKnowledgeGraph(
    size_t& inputCol, std::shared_ptr<QueryExecutionTree> leftOrRightOp) const {
  auto [originalVar, info] =
      leftOrRightOp->getVariableAndInfoByColumnIndex(inputCol);

  // If we're not explicitly handling the empty path, the first step will
  // already filter out non-matching values.
  if (minDist_ > 0) {
    return leftOrRightOp;
  }

  // Remove undef values, these are definitely not in the graph, and are
  // problematic when joining.
  if (info.mightContainUndef_ != ColumnIndexAndTypeInfo::AlwaysDefined) {
    using namespace sparqlExpression;
    SparqlExpressionPimpl pimpl{
        makeBoundExpression(std::make_unique<VariableExpression>(originalVar)),
        absl::StrCat("BOUND(", originalVar.name(), ")")};
    leftOrRightOp = ad_utility::makeExecutionTree<Filter>(
        getExecutionContext(), std::move(leftOrRightOp), std::move(pimpl));
    AD_CORRECTNESS_CHECK(
        inputCol == leftOrRightOp->getVariableColumn(originalVar),
        "The column index should not change when applying a filter.");
  }

  // If we cannot guarantee the values are part of the graph, we have to join
  // with it first.
  if (!leftOrRightOp->getRootOperation()->columnOriginatesFromGraphOrUndef(
          originalVar)) {
    leftOrRightOp = ad_utility::makeExecutionTree<Join>(
        getExecutionContext(), std::move(leftOrRightOp),
        makeEmptyPathSide(getExecutionContext(), activeGraphs_, originalVar),
        inputCol, 0);
    inputCol = leftOrRightOp->getVariableColumn(originalVar);
  }
  return leftOrRightOp;
}

// _____________________________________________________________________________
std::shared_ptr<TransitivePathBase> TransitivePathBase::bindLeftOrRightSide(
    std::shared_ptr<QueryExecutionTree> leftOrRightOp, size_t inputCol,
    bool isLeft) const {
  leftOrRightOp = matchWithKnowledgeGraph(inputCol, std::move(leftOrRightOp));
  // Enforce required sorting of `leftOrRightOp`.
  leftOrRightOp = QueryExecutionTree::createSortedTree(std::move(leftOrRightOp),
                                                       {inputCol});
  // Create a copy of this.
  //
  // NOTE: The RHS used to be `std::make_shared<TransitivePath>()`, which is
  // wrong because it first calls the copy constructor of the base class
  // `Operation`, which  would then ignore the changes in `variableColumnMap_`
  // made below (see `Operation::getInternallyVisibleVariableColumns` and
  // `Operation::getExternallyVariableColumns`).
  auto lhs = lhs_;
  auto rhs = rhs_;
  if (isLeft) {
    lhs.treeAndCol_ = {leftOrRightOp, inputCol};
    // Remove placeholder tree if binding actual tree.
    if (!rhs.isVariable()) {
      rhs.treeAndCol_ = std::nullopt;
    }
  } else {
    // Remove placeholder tree if binding actual tree.
    if (boundVariableIsForEmptyPath_ || !lhs.isVariable()) {
      lhs.treeAndCol_ = std::nullopt;
    }
    rhs.treeAndCol_ = {leftOrRightOp, inputCol};
  }

  // We use the cheapest tree that can be created using any of the alternative
  // subtrees. This has the effect that the `TransitivePathBinSearch` will
  // never re-sort an index scan (which should not happen because we can just
  // take the appropriate index scan in the first place).
  bool useBinSearch = dynamic_cast<const TransitivePathBinSearch*>(this);
  std::vector<std::shared_ptr<TransitivePathBase>> candidates;
  candidates.push_back(makeTransitivePath(getExecutionContext(), subtree_, lhs,
                                          rhs, minDist_, maxDist_, useBinSearch,
                                          {}));
  for (const auto& alternativeSubtree : alternativeSubtrees()) {
    candidates.push_back(
        makeTransitivePath(getExecutionContext(), alternativeSubtree, lhs, rhs,
                           minDist_, maxDist_, useBinSearch, {}));
  }

  auto& p = *ql::ranges::min_element(
      candidates, {}, [](const auto& tree) { return tree->getCostEstimate(); });

  // Note: The `variable` in the following structured binding is `const`, even
  // if we bind by value. We deliberately make one unnecessary copy of the
  // `variable` to keep the code simpler.
  for (auto [variable, columnIndexWithType] :
       leftOrRightOp->getVariableColumns()) {
    ColumnIndex columnIndex = columnIndexWithType.columnIndex_;
    if (columnIndex == inputCol) {
      continue;
    }

    columnIndexWithType.columnIndex_ += columnIndex > inputCol ? 1 : 2;

    AD_CORRECTNESS_CHECK(!p->variableColumns_.contains(variable));
    p->variableColumns_[variable] = columnIndexWithType;
  }
  p->resultWidth_ += leftOrRightOp->getResultWidth() - 1;
  return std::move(p);
}

// _____________________________________________________________________________
bool TransitivePathBase::isBoundOrId() const {
  // Don't make the execution tree for the empty path count as "bound".
  return !boundVariableIsForEmptyPath_ &&
         (!lhs_.isUnboundVariable() || !rhs_.isUnboundVariable());
}

// _____________________________________________________________________________
template <size_t INPUT_WIDTH, size_t OUTPUT_WIDTH>
void TransitivePathBase::copyColumns(const IdTableView<INPUT_WIDTH>& inputTable,
                                     IdTableStatic<OUTPUT_WIDTH>& outputTable,
                                     size_t inputRow, size_t outputRow) {
  size_t inCol = 0;
  size_t outCol = 2;
  AD_CORRECTNESS_CHECK(inputTable.numColumns() + 2 == outputTable.numColumns());
  while (inCol < inputTable.numColumns() && outCol < outputTable.numColumns()) {
    outputTable.at(outputRow, outCol) = inputTable.at(inputRow, inCol);
    inCol++;
    outCol++;
  }
}

// _____________________________________________________________________________
bool TransitivePathBase::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  return variable == lhs_.value_ || variable == rhs_.value_;
}
