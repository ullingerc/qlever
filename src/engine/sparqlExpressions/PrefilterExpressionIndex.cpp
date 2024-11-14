//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/PrefilterExpressionIndex.h"

#include <ranges>

#include "global/ValueIdComparators.h"

namespace prefilterExpressions {

// HELPER FUNCTIONS
//______________________________________________________________________________
// Given a PermutedTriple retrieve the suitable Id w.r.t. a column (index).
static Id getIdFromColumnIndex(const BlockMetadata::PermutedTriple& triple,
                               size_t columnIndex) {
  switch (columnIndex) {
    case 0:
      return triple.col0Id_;
    case 1:
      return triple.col1Id_;
    case 2:
      return triple.col2Id_;
    default:
      // columnIndex out of bounds
      AD_FAIL();
  }
};

//______________________________________________________________________________
// Extract the Ids from the given `PermutedTriple` in a tuple w.r.t. the
// position (column index) defined by `ignoreIndex`. The ignored positions are
// filled with Ids `Id::min()`. `Id::min()` is guaranteed
// to be smaller than Ids of all other types.
static auto getMaskedTriple(const BlockMetadata::PermutedTriple& triple,
                            size_t ignoreIndex = 3) {
  const Id& undefined = Id::min();
  switch (ignoreIndex) {
    case 3:
      return std::make_tuple(triple.col0Id_, triple.col1Id_, triple.col2Id_);
    case 2:
      return std::make_tuple(triple.col0Id_, triple.col1Id_, undefined);
    case 1:
      return std::make_tuple(triple.col0Id_, undefined, undefined);
    case 0:
      return std::make_tuple(undefined, undefined, undefined);
    default:
      // ignoreIndex out of bounds
      AD_FAIL();
  }
};

//______________________________________________________________________________
// Check required conditions.
static void checkEvalRequirements(const std::vector<BlockMetadata>& input,
                                  size_t evaluationColumn) {
  const auto throwRuntimeError = [](const std::string& errorMessage) {
    throw std::runtime_error(errorMessage);
  };
  // Check for duplicates.
  if (auto it = std::ranges::adjacent_find(input); it != input.end()) {
    throwRuntimeError("The provided data blocks must be unique.");
  }
  // Helper to check for fully sorted blocks. Return `true` if `b1 < b2` is
  // satisfied.
  const auto checkOrder = [](const BlockMetadata& b1, const BlockMetadata& b2) {
    if (b1.blockIndex_ < b2.blockIndex_) {
      AD_CORRECTNESS_CHECK(getMaskedTriple(b1.lastTriple_) <=
                           getMaskedTriple(b2.lastTriple_));
      return true;
    }
    if (b1.blockIndex_ == b2.blockIndex_) {
      // Given the previous check detects duplicates in the input, the
      // correctness check here will never evaluate to true.
      // => blockIndex_ assignment issue.
      AD_CORRECTNESS_CHECK(b1 == b2);
    } else {
      AD_CORRECTNESS_CHECK(getMaskedTriple(b1.lastTriple_) >
                           getMaskedTriple(b2.firstTriple_));
    }
    return false;
  };
  if (!std::ranges::is_sorted(input, checkOrder)) {
    throwRuntimeError("The blocks must be provided in sorted order.");
  }
  // Helper to check for column consistency. Returns `true` if the columns for
  // `b1` and `b2` up to the evaluation are inconsistent.
  const auto checkColumnConsistency =
      [evaluationColumn](const BlockMetadata& b1, const BlockMetadata& b2) {
        const auto& b1Last = getMaskedTriple(b1.lastTriple_, evaluationColumn);
        const auto& b2First =
            getMaskedTriple(b2.firstTriple_, evaluationColumn);
        return getMaskedTriple(b1.firstTriple_, evaluationColumn) != b1Last ||
               b1Last != b2First ||
               b2First != getMaskedTriple(b2.lastTriple_, evaluationColumn);
      };
  if (auto it = std::ranges::adjacent_find(input, checkColumnConsistency);
      it != input.end()) {
    throwRuntimeError(
        "The values in the columns up to the evaluation column must be "
        "consistent.");
  }
};

//______________________________________________________________________________
// Given two sorted `vector`s containing `BlockMetadata`, this function
// returns their merged `BlockMetadata` content in a `vector` which is free of
// duplicates and ordered.
static auto getSetUnion(const std::vector<BlockMetadata>& blocks1,
                        const std::vector<BlockMetadata>& blocks2) {
  std::vector<BlockMetadata> mergedVectors;
  mergedVectors.reserve(blocks1.size() + blocks2.size());
  const auto blockLessThanBlock = [](const BlockMetadata& b1,
                                     const BlockMetadata& b2) {
    return b1.blockIndex_ < b2.blockIndex_;
  };
  // Given that we have vectors with sorted (BlockMedata) values, we can
  // use std::ranges::set_union. Thus the complexity is O(n + m).
  std::ranges::set_union(blocks1, blocks2, std::back_inserter(mergedVectors),
                         blockLessThanBlock);
  mergedVectors.shrink_to_fit();
  return mergedVectors;
}

//______________________________________________________________________________
// Return `CompOp`s as string.
static std::string getRelationalOpStr(const CompOp relOp) {
  using enum CompOp;
  switch (relOp) {
    case LT:
      return "LT(<)";
    case LE:
      return "LE(<=)";
    case EQ:
      return "EQ(=)";
    case NE:
      return "NE(!=)";
    case GE:
      return "GE(>=)";
    case GT:
      return "GT(>)";
    default:
      AD_FAIL();
  }
}

//______________________________________________________________________________
// Return `LogicalOperator`s as string.
static std::string getLogicalOpStr(const LogicalOperator logOp) {
  using enum LogicalOperator;
  switch (logOp) {
    case AND:
      return "AND(&&)";
    case OR:
      return "OR(||)";
    default:
      AD_FAIL();
  }
}

// SECTION PREFILTER EXPRESSION (BASE CLASS)
//______________________________________________________________________________
std::vector<BlockMetadata> PrefilterExpression::evaluate(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  checkEvalRequirements(input, evaluationColumn);
  const auto& relevantBlocks = evaluateImpl(input, evaluationColumn);
  checkEvalRequirements(relevantBlocks, evaluationColumn);
  return relevantBlocks;
};

// SECTION RELATIONAL OPERATIONS
//______________________________________________________________________________
template <CompOp Comparison>
std::unique_ptr<PrefilterExpression>
RelationalExpression<Comparison>::logicalComplement() const {
  using enum CompOp;
  switch (Comparison) {
    case LT:
      // Complement X < Y: X >= Y
      return std::make_unique<GreaterEqualExpression>(referenceId_);
    case LE:
      // Complement X <= Y: X > Y
      return std::make_unique<GreaterThanExpression>(referenceId_);
    case EQ:
      // Complement X == Y: X != Y
      return std::make_unique<NotEqualExpression>(referenceId_);
    case NE:
      // Complement X != Y: X == Y
      return std::make_unique<EqualExpression>(referenceId_);
    case GE:
      // Complement X >= Y: X < Y
      return std::make_unique<LessThanExpression>(referenceId_);
    case GT:
      // Complement X > Y: X <= Y
      return std::make_unique<LessEqualExpression>(referenceId_);
    default:
      AD_FAIL();
  }
};

//______________________________________________________________________________
template <CompOp Comparison>
std::vector<BlockMetadata> RelationalExpression<Comparison>::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  using namespace valueIdComparators;
  std::vector<ValueId> valueIdsInput;
  // For each BlockMetadata value in vector input, we have a respective Id for
  // firstTriple and lastTriple
  valueIdsInput.reserve(2 * input.size());
  std::vector<BlockMetadata> mixedDatatypeBlocks;

  for (const auto& block : input) {
    const auto firstId =
        getIdFromColumnIndex(block.firstTriple_, evaluationColumn);
    const auto secondId =
        getIdFromColumnIndex(block.lastTriple_, evaluationColumn);
    valueIdsInput.push_back(firstId);
    valueIdsInput.push_back(secondId);

    if (firstId.getDatatype() != secondId.getDatatype()) {
      mixedDatatypeBlocks.push_back(block);
    }
  }

  // Use getRangesForId (from valueIdComparators) to extract the ranges
  // containing the relevant ValueIds.
  // For pre-filtering with CompOp::EQ, we have to consider empty ranges.
  // Reason: The referenceId_ could be contained within the bounds formed by
  // the IDs of firstTriple_ and lastTriple_ (set false flag to keep
  // empty ranges).
  auto relevantIdRanges =
      Comparison != CompOp::EQ
          ? getRangesForId(valueIdsInput.begin(), valueIdsInput.end(),
                           referenceId_, Comparison)
          : getRangesForId(valueIdsInput.begin(), valueIdsInput.end(),
                           referenceId_, Comparison, false);

  // The vector for relevant BlockMetadata values which contain ValueIds
  // defined as relevant by relevantIdRanges.
  std::vector<BlockMetadata> relevantBlocks;
  // Reserve memory, input.size() is upper bound.
  relevantBlocks.reserve(input.size());

  // Given the relevant Id ranges, retrieve the corresponding relevant
  // BlockMetadata values from vector input and add them to the relevantBlocks
  // vector.
  auto endValueIdsInput = valueIdsInput.end();
  for (const auto& [firstId, secondId] : relevantIdRanges) {
    // Ensures that index is within bounds of index vector.
    auto secondIdAdjusted =
        secondId < endValueIdsInput ? secondId + 1 : secondId;
    relevantBlocks.insert(
        relevantBlocks.end(),
        input.begin() + std::distance(valueIdsInput.begin(), firstId) / 2,
        // Round up, for Ids contained within the bounding Ids of firstTriple
        // and lastTriple we have to include the respective metadata block
        // (that block is partially relevant).
        input.begin() +
            std::distance(valueIdsInput.begin(), secondIdAdjusted) / 2);
  }
  relevantBlocks.shrink_to_fit();
  // Merge mixedDatatypeBlocks into relevantBlocks while maintaining order and
  // avoiding duplicates.
  return getSetUnion(relevantBlocks, mixedDatatypeBlocks);
};

//______________________________________________________________________________
template <CompOp Comparison>
bool RelationalExpression<Comparison>::operator==(
    const PrefilterExpression& other) const {
  const auto* otherRelational =
      dynamic_cast<const RelationalExpression<Comparison>*>(&other);
  if (!otherRelational) {
    return false;
  }
  return referenceId_ == otherRelational->referenceId_;
};

//______________________________________________________________________________
template <CompOp Comparison>
std::unique_ptr<PrefilterExpression> RelationalExpression<Comparison>::clone()
    const {
  return std::make_unique<RelationalExpression<Comparison>>(referenceId_);
};

//______________________________________________________________________________
template <CompOp Comparison>
std::string RelationalExpression<Comparison>::asString(
    [[maybe_unused]] size_t depth) const {
  std::stringstream stream;
  stream << "Prefilter RelationalExpression<" << getRelationalOpStr(Comparison)
         << ">\nValueId: " << referenceId_ << std::endl;
  return stream.str();
};

// SECTION LOGICAL OPERATIONS
//______________________________________________________________________________
template <LogicalOperator Operation>
std::unique_ptr<PrefilterExpression>
LogicalExpression<Operation>::logicalComplement() const {
  using enum LogicalOperator;
  // Source De-Morgan's laws: De Morgan's laws, Wikipedia.
  // Reference: https://en.wikipedia.org/wiki/De_Morgan%27s_laws
  if constexpr (Operation == OR) {
    // De Morgan's law: not (A or B) = (not A) and (not B)
    return std::make_unique<AndExpression>(child1_->logicalComplement(),
                                           child2_->logicalComplement());
  } else {
    static_assert(Operation == AND);
    // De Morgan's law: not (A and B) = (not A) or (not B)
    return std::make_unique<OrExpression>(child1_->logicalComplement(),
                                          child2_->logicalComplement());
  }
};

//______________________________________________________________________________
template <LogicalOperator Operation>
std::vector<BlockMetadata> LogicalExpression<Operation>::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  using enum LogicalOperator;
  if constexpr (Operation == AND) {
    auto resultChild1 = child1_->evaluate(input, evaluationColumn);
    return child2_->evaluate(resultChild1, evaluationColumn);
  } else {
    static_assert(Operation == OR);
    return getSetUnion(child1_->evaluate(input, evaluationColumn),
                       child2_->evaluate(input, evaluationColumn));
  }
};

//______________________________________________________________________________
template <LogicalOperator Operation>
bool LogicalExpression<Operation>::operator==(
    const PrefilterExpression& other) const {
  const auto* otherlogical =
      dynamic_cast<const LogicalExpression<Operation>*>(&other);
  if (!otherlogical) {
    return false;
  }
  return *child1_ == *otherlogical->child1_ &&
         *child2_ == *otherlogical->child2_;
};

//______________________________________________________________________________
template <LogicalOperator Operation>
std::unique_ptr<PrefilterExpression> LogicalExpression<Operation>::clone()
    const {
  return std::make_unique<LogicalExpression<Operation>>(child1_->clone(),
                                                        child2_->clone());
};

//______________________________________________________________________________
template <LogicalOperator Operation>
std::string LogicalExpression<Operation>::asString(size_t depth) const {
  std::string child1Info =
      depth < maxInfoRecursion ? child1_->asString(depth + 1) : "MAX_DEPTH";
  std::string child2Info =
      depth < maxInfoRecursion ? child2_->asString(depth + 1) : "MAX_DEPTH";
  std::stringstream stream;
  stream << "Prefilter LogicalExpression<" << getLogicalOpStr(Operation)
         << ">\n"
         << "child1 {" << child1Info << "}"
         << "child2 {" << child2Info << "}" << std::endl;
  return stream.str();
};

// SECTION NOT-EXPRESSION
//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> NotExpression::logicalComplement() const {
  // Logically we complement (negate) a NOT here => NOT cancels out.
  // Therefore, we can simply return the child of the respective NOT
  // expression after undoing its previous complementation.
  return child_->logicalComplement();
};

//______________________________________________________________________________
std::vector<BlockMetadata> NotExpression::evaluateImpl(
    const std::vector<BlockMetadata>& input, size_t evaluationColumn) const {
  return child_->evaluate(input, evaluationColumn);
};

//______________________________________________________________________________
bool NotExpression::operator==(const PrefilterExpression& other) const {
  const auto* otherNotExpression = dynamic_cast<const NotExpression*>(&other);
  if (!otherNotExpression) {
    return false;
  }
  return *child_ == *otherNotExpression->child_;
}

//______________________________________________________________________________
std::unique_ptr<PrefilterExpression> NotExpression::clone() const {
  return std::make_unique<NotExpression>((child_->clone()), true);
};

//______________________________________________________________________________
std::string NotExpression::asString(size_t depth) const {
  std::string childInfo =
      depth < maxInfoRecursion ? child_->asString(depth + 1) : "MAX_DEPTH";
  std::stringstream stream;
  stream << "Prefilter NotExpression:\n"
         << "child {" << childInfo << "}" << std::endl;
  return stream.str();
}

//______________________________________________________________________________
// Instanitate templates with specializations (for linking accessibility)
template class RelationalExpression<CompOp::LT>;
template class RelationalExpression<CompOp::LE>;
template class RelationalExpression<CompOp::GE>;
template class RelationalExpression<CompOp::GT>;
template class RelationalExpression<CompOp::EQ>;
template class RelationalExpression<CompOp::NE>;

template class LogicalExpression<LogicalOperator::AND>;
template class LogicalExpression<LogicalOperator::OR>;

namespace detail {
//______________________________________________________________________________
void checkPropertiesForPrefilterConstruction(
    const std::vector<PrefilterExprVariablePair>& vec) {
  auto viewVariable = vec | std::views::values;
  if (!std::ranges::is_sorted(viewVariable, std::less<>{})) {
    throw std::runtime_error(
        "The vector must contain the <PrefilterExpression, Variable> pairs in "
        "sorted order w.r.t. Variable value.");
  }
  if (auto it = std::ranges::adjacent_find(viewVariable);
      it != std::ranges::end(viewVariable)) {
    throw std::runtime_error(
        "For each relevant Variable must exist exactly one "
        "<PrefilterExpression, Variable> pair.");
  }
}

}  //  namespace detail
}  //  namespace prefilterExpressions