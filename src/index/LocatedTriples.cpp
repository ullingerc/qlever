// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/LocatedTriples.h"

#include "backports/algorithm.h"
#include "global/RuntimeParameters.h"
#include "index/CompressedRelationMetadata.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/GraphComputation.h"
#include "index/Permutation.h"
#include "util/ChunkedForLoop.h"
#include "util/Log.h"
#include "util/ValueIdentity.h"

// ____________________________________________________________________________
size_t LocatedTriple::locateBlock(
    const IdTriple<0>& permutedTriple,
    ql::span<const CompressedBlockMetadata> blockMetadata) {
  // A triple belongs to the first block that contains at least one triple
  // that larger than or equal to the triple. See `LocatedTriples.h` for a
  // discussion of the corner cases.
  //
  // NOTE: Only the first three columns are compared. This is also correct for
  // rows with payload columns (materialized views), because the
  // `CompressedRelationWriter` never splits rows that are equal in their first
  // three columns across blocks (see `PermutationWriter::
  // addRowsOfCurrentRelation` and `CompressedRelationWriter::
  // addCompleteLargeRelation`), so all rows with the same key (independent of
  // the graph and payload) are in the same block.
  return ql::ranges::lower_bound(
             blockMetadata, permutedTriple.toPermutedTriple(),
             [](const auto& a, const auto& b) {
               // All identical triples with different graphs are currently
               // stored in the same block, so we don't need to check the
               // graph. In particular, if this triple is equal (without
               // graphs) to the first or last triple of a block, then this
               // call to `lower_bound` will correctly identify this block.
               return a.tieWithoutGraph() < b.tieWithoutGraph();
             },
             &CompressedBlockMetadata::lastTriple_) -
         blockMetadata.begin();
}

namespace {
// Common implementation of `locateTriplesInPermutation` and `locateRowsInView`:
// Locate `numRows` rows, where `getRow(i)` returns a pair of the key (in the
// order of the permutation) and the payload of the `i`-th row.
template <typename GetRow>
std::vector<LocatedTriple> locateRows(
    size_t numRows, ql::span<const CompressedBlockMetadata> blockMetadata,
    bool insertOrDelete,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const GetRow& getRow) {
  std::vector<LocatedTriple> out;
  out.reserve(numRows);
  ad_utility::chunkedForLoop<10'000>(
      0, numRows,
      [&out, &blockMetadata, insertOrDelete, &getRow](size_t i) {
        auto [triple, payload] = getRow(i);
        size_t blockIndex = LocatedTriple::locateBlock(triple, blockMetadata);
        out.push_back({blockIndex, std::move(triple), std::move(payload),
                       insertOrDelete});
      },
      [&cancellationHandle]() { cancellationHandle->throwIfCancelled(); });
  return out;
}
}  // namespace

// ____________________________________________________________________________
std::vector<LocatedTriple> LocatedTriple::locateTriplesInPermutation(
    ql::span<const IdTriple<0>> triples,
    ql::span<const CompressedBlockMetadata> blockMetadata,
    const qlever::KeyOrder& keyOrder, bool insertOrDelete,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  return locateRows(triples.size(), blockMetadata, insertOrDelete,
                    cancellationHandle, [&triples, &keyOrder](size_t i) {
                      return std::pair{triples[i].permute(keyOrder),
                                       std::vector<Id>{}};
                    });
}

// ____________________________________________________________________________
std::vector<LocatedTriple> LocatedTriple::locateRowsInView(
    const IdTable& rows, ql::span<const CompressedBlockMetadata> blockMetadata,
    bool insertOrDelete,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  AD_CONTRACT_CHECK(rows.numColumns() >= 4);
  return locateRows(
      rows.numRows(), blockMetadata, insertOrDelete, cancellationHandle,
      [&rows](size_t i) {
        const auto& row = rows[i];
        return std::pair{
            IdTriple<0>{std::array{row[0], row[1], row[2], row[3]}},
            std::vector<Id>(row.begin() + 4, row.end())};
      });
}

// ____________________________________________________________________________
boost::optional<const LocatedTriples&>
LocatedTriplesPerBlock::getUpdatesIfPresent(size_t blockIndex) const {
  auto it = map_.find(blockIndex);
  if (it == map_.end()) {
    return boost::optional<const LocatedTriples&>{};
  }
  return boost::optional<const LocatedTriples&>{it->second};
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::consolidateAllBlocks() {
  ql::ranges::for_each(map_ | ql::views::values,
                       [](auto& lts) { lts.consolidate(); });
}

// ____________________________________________________________________________
NumAddedAndDeleted LocatedTriplesPerBlock::numTriples(size_t blockIndex) const {
  if (auto blockUpdateTriples = getUpdatesIfPresent(blockIndex)) {
    // Simply return the number of located triples twice. See the comment in the
    // header file for the reasons and potential improvements.
    return {blockUpdateTriples->sizeUpperBound(),
            blockUpdateTriples->sizeUpperBound()};
  }
  return {0, 0};
}

namespace {

// This code works for `std::integer_sequence` as well as
// `ad_utility::ValueSequence`.
template <typename Row, template <typename T, T...> typename Tp, size_t... I>
auto tieHelper(Row& row, Tp<size_t, I...>) {
  return std::tie(row[I]...);
}
}  // namespace

// Return a `std::tie` of the relevant entries of a row, according to
// `numIndexColumns` and `includeGraphColumn`. For example, if `numIndexColumns`
// is `2` and `includeGraphColumn` is `true`, the function returns
// `std::tie(row[0], row[1], row[2])`.
CPP_template(size_t numIndexColumns, bool includeGraphColumn,
             typename T)(requires(numIndexColumns >= 1 &&
                                  numIndexColumns <=
                                      3)) auto tieIdTableRow(T& row) {
  return tieHelper(
      row, std::make_index_sequence<numIndexColumns +
                                    static_cast<size_t>(includeGraphColumn)>{});
}

// Return a `std::tie` of the relevant entries of a located triple,
// according to `numIndexColumns` and `includeGraphColumn`. For example, if
// `numIndexColumns` is `2` and `includeGraphColumn` is `true`, the function
// returns `std::tie(ids_[1], ids_[2], ids_[3])`, where `ids_` is from
// `lt->triple_`.
template <size_t numIndexColumns, bool includeGraphColumn>
static constexpr auto tieLocatedTriplesIndices = []() {
  std::array<size_t, numIndexColumns + static_cast<size_t>(includeGraphColumn)>
      a{};
  for (size_t i = 0; i < a.size(); ++i) {
    a[i] = i + (3 - numIndexColumns);
  }
  return a;
}();

// Return a `std::tie` of the relevant entries of the `const LocatedTriple& lt`.
CPP_template(size_t numIndexColumns, bool includeGraphColumn,
             typename T)(requires(numIndexColumns >= 1 &&
                                  numIndexColumns <=
                                      3)) auto tieLocatedTripleValue(T& lt) {
  const auto& ids = lt.triple_.ids();
  return tieHelper(
      ids,
      ad_utility::toIntegerSequenceRef<
          tieLocatedTriplesIndices<numIndexColumns, includeGraphColumn>>());
}

namespace {
// Three-way merge of the sorted `block` with the sorted `locatedTriples`, which
// is the common implementation of `mergeTriplesImpl` and `mergeFullRows`.
// `compare(lt, row)` returns a negative value, zero, or a positive value if the
// located triple `lt` is less than, equal to, or greater than the `row` of the
// `block`. `writeLocatedTriple(lt, resultIt)` writes the inserted located
// triple `lt` to the row of the result that `resultIt` points to. `numAdded` is
// an upper bound for the number of inserted located triples.
template <typename Compare, typename WriteLocatedTriple>
IdTable mergeBlockAndLocatedTriples(
    const IdTable& block, const LocatedTriples& locatedTriples, size_t numAdded,
    const Compare& compare, const WriteLocatedTriple& writeLocatedTriple) {
  IdTable result{block.numColumns(), block.getAllocator()};
  result.resize(block.numRows() + numAdded);

  auto rowIt = block.begin();
  auto sortedLocatedTriples = locatedTriples.getSortedView();
  auto locatedTripleIt = sortedLocatedTriples.begin();
  auto locatedTripleEnd = sortedLocatedTriples.end();
  auto resultIt = result.begin();

  // Write the given `locatedTriple` to `result` at position `resultIt` and
  // advance `resultIt` by one.
  auto writeLocatedTripleToResult =
      [&resultIt, &writeLocatedTriple](const LocatedTriple& locatedTriple) {
        writeLocatedTriple(locatedTriple, resultIt);
        resultIt++;
      };

  while (rowIt != block.end() && locatedTripleIt != locatedTripleEnd) {
    int cmp = compare(*locatedTripleIt, *rowIt);
    if (cmp < 0) {
      if (locatedTripleIt->insertOrDelete_) {
        // Insertion of a non-existent triple.
        writeLocatedTripleToResult(*locatedTripleIt);
      }
      locatedTripleIt++;
    } else if (cmp == 0) {
      if (!locatedTripleIt->insertOrDelete_) {
        // Deletion of an existing triple.
        rowIt++;
      }
      locatedTripleIt++;
    } else {
      // The rowIt is not deleted - copy it
      *resultIt++ = *rowIt++;
    }
  }

  if (locatedTripleIt != locatedTripleEnd) {
    AD_CORRECTNESS_CHECK(rowIt == block.end());
    ql::ranges::for_each(
        ql::ranges::subrange(locatedTripleIt, locatedTripleEnd) |
            ql::views::filter(&LocatedTriple::insertOrDelete_),
        writeLocatedTripleToResult);
  }
  if (rowIt != block.end()) {
    AD_CORRECTNESS_CHECK(locatedTripleIt == locatedTripleEnd);
    while (rowIt != block.end()) {
      *resultIt++ = *rowIt++;
    }
  }

  result.resize(resultIt - result.begin());
  return result;
}

// Three-way comparison of the full row of `lt` (key and payload) with the
// full `row` of a materialized view. Returns a negative value, zero, or a
// positive value.
constexpr auto compareFullRow = [](const LocatedTriple& lt, const auto& row) {
  auto ltKey = tieLocatedTripleValue<3, true>(lt);
  auto rowKey = tieIdTableRow<3, true>(row);
  if (ltKey != rowKey) {
    return ltKey < rowKey ? -1 : 1;
  }
  for (size_t i = 0; i < lt.payload_.size(); ++i) {
    if (lt.payload_[i] != row[4 + i]) {
      return lt.payload_[i] < row[4 + i] ? -1 : 1;
    }
  }
  return 0;
};
}  // namespace

// ____________________________________________________________________________
template <size_t numIndexColumns, bool includeGraphColumn>
IdTable LocatedTriplesPerBlock::mergeTriplesImpl(size_t blockIndex,
                                                 const IdTable& block) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  AD_CONTRACT_CHECK(numIndexColumns + static_cast<size_t>(includeGraphColumn) <=
                    block.numColumns());

  auto compare = [](const LocatedTriple& lt, const auto& row) {
    auto ltKey = tieLocatedTripleValue<numIndexColumns, includeGraphColumn>(lt);
    auto rowKey = tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
    return ltKey < rowKey ? -1 : (ltKey == rowKey ? 0 : 1);
  };

  // Write the given `locatedTriple` to the row that `resultIt` points to. See
  // the example in the comment of the declaration of `mergeTriples` to
  // understand the behavior of this function.
  auto writeLocatedTriple = [numColumns = block.numColumns()](
                                const LocatedTriple& locatedTriple,
                                const auto& resultIt) {
    // Write part from `locatedTriple` that also occurs in the input `block` to
    // the result.
    static constexpr auto plusOneIfGraph =
        static_cast<size_t>(includeGraphColumn);
    for (size_t i = 0; i < numIndexColumns + plusOneIfGraph; i++) {
      (*resultIt)[i] = locatedTriple.triple_.ids()[3 - numIndexColumns + i];
    }
    // If the input `block` has payload columns (which located triples don't
    // have), set their values to UNDEF.
    for (size_t i = numIndexColumns + plusOneIfGraph; i < numColumns; i++) {
      (*resultIt)[i] = ValueId::makeUndefined();
    }
  };

  return mergeBlockAndLocatedTriples(block, map_.at(blockIndex),
                                     numTriples(blockIndex).numAdded_, compare,
                                     writeLocatedTriple);
}

// ____________________________________________________________________________
IdTable LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                             const IdTable& block,
                                             size_t numIndexColumns,
                                             bool includeGraphColumn) const {
  // The following code does nothing more than turn `numIndexColumns` and
  // `includeGraphColumn` into template parameters of `mergeTriplesImpl`.
  auto mergeTriplesImplHelper = [numIndexColumns, blockIndex, &block,
                                 this](auto hasGraphColumn) {
    if (numIndexColumns == 3) {
      return mergeTriplesImpl<3, hasGraphColumn>(blockIndex, block);
    } else if (numIndexColumns == 2) {
      return mergeTriplesImpl<2, hasGraphColumn>(blockIndex, block);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return mergeTriplesImpl<1, hasGraphColumn>(blockIndex, block);
    }
  };
  using ad_utility::use_value_identity::vi;
  if (includeGraphColumn) {
    return mergeTriplesImplHelper(vi<true>);
  } else {
    return mergeTriplesImplHelper(vi<false>);
  }
}

// ____________________________________________________________________________
IdTable LocatedTriplesPerBlock::mergeFullRows(size_t blockIndex,
                                              const IdTable& block) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));
  AD_CONTRACT_CHECK(block.numColumns() == 4 + numPayloadColumns_);

  // Write the full row of `lt` (key and payload) to the row that `resultIt`
  // points to.
  auto writeLocatedTriple = [this](const LocatedTriple& lt,
                                   const auto& resultIt) {
    for (size_t i = 0; i < 4; ++i) {
      (*resultIt)[i] = lt.triple_.ids()[i];
    }
    for (size_t i = 0; i < numPayloadColumns_; ++i) {
      (*resultIt)[4 + i] = lt.payload_[i];
    }
  };

  return mergeBlockAndLocatedTriples(block, map_.at(blockIndex),
                                     numTriples(blockIndex).numAdded_,
                                     compareFullRow, writeLocatedTriple);
}

namespace {
// The indices of the blocks in `map` that have at least
// `vacuum-minimum-block-size` located triples.
std::vector<size_t> blocksToVacuum(
    const ad_utility::HashMap<size_t, LocatedTriples>& map) {
  size_t minimumBlockSize =
      getRuntimeParameter<&RuntimeParameters::vacuumMinimumBlockSize_>();
  return ::ranges::to_vector(
      map | ql::views::filter([minimumBlockSize](const auto& e) {
        return e.second.sizeUpperBound() >= minimumBlockSize;
      }) |
      ql::views::keys);
}

// Identify the triples to vacuum for a single block by comparing the
// `locatedTriples` with the `idTable` of the block (which has no updates
// applied).
VacuumStatistics processBlockForVacuum(
    const IdTable& idTable, const LocatedTriples& locatedTriples,
    const qlever::KeyOrder::Array& inverseKeys,
    std::vector<IdTriple<0>>& allDeletionsToRemove,
    std::vector<IdTriple<0>>& allInsertionsToRemove) {
  auto toSpo = [&inverseKeys](auto& ids) {
    return IdTriple<0>{[&]<size_t... I>(std::index_sequence<I...>) {
      std::array<Id, 4> spo{};
      ((spo[inverseKeys[I]] = std::get<I>(ids)), ...);
      return spo;
    }(std::make_index_sequence<4>{})};
  };

  auto ltProj = [](const LocatedTriple& lt)
      -> std::tuple<const Id&, const Id&, const Id&, const Id&> {
    return tieLocatedTripleValue<3, true>(lt);
  };
  auto rowProj = [](const auto& row)
      -> std::tuple<const Id&, const Id&, const Id&, const Id&> {
    return tieIdTableRow<3, true>(row);
  };

  auto rowsAsTuple = idTable | ql::views::transform(rowProj);
  auto filteredTriples = [&](bool isInsertion) {
    return locatedTriples.getSortedView() |
           ql::views::filter([isInsertion](const LocatedTriple& lt) {
             return lt.insertOrDelete_ == isInsertion;
           }) |
           ql::views::transform(ltProj);
  };
  auto processTriples = [&](bool isInsertion, auto setAlgorithm,
                            std::vector<IdTriple<0>>& target) {
    size_t before = target.size();
    setAlgorithm(filteredTriples(isInsertion), rowsAsTuple,
                 ad_utility::IteratorForAssigmentOperator{
                     [&](auto&& ids) { target.push_back(toSpo(ids)); }});
    return target.size() - before;
  };

  auto insertionsRemovedInBlock =
      processTriples(true, ql::ranges::set_intersection, allInsertionsToRemove);
  auto deletionsRemovedInBlock =
      processTriples(false, ql::ranges::set_difference, allDeletionsToRemove);

  // TODO<qup42>: we could also get these without an extra iteration by
  // instrumenting the iteration in `processTriples`.
  size_t insertionsInBlock = 0, deletionsInBlock = 0;
  ql::ranges::for_each(locatedTriples.getSortedView(),
                       [&](const LocatedTriple& lt) {
                         if (lt.insertOrDelete_) {
                           insertionsInBlock++;
                         } else {
                           deletionsInBlock++;
                         }
                       });

  return {deletionsRemovedInBlock, insertionsRemovedInBlock,
          deletionsInBlock - deletionsRemovedInBlock,
          insertionsInBlock - insertionsRemovedInBlock};
}
}  // namespace

// ____________________________________________________________________________
TriplesToVacuum LocatedTriplesPerBlock::identifyTriplesToVacuum(
    const Permutation& perm,
    ad_utility::SharedCancellationHandle cancellationHandle) const {
  VacuumStatistics totalStats{0, 0, 0, 0};
  std::vector<IdTriple<0>> allDeletionsToRemove;
  std::vector<IdTriple<0>> allInsertionsToRemove;

  // The identified triples are output in `SPO` so we need to invert the
  // permutation.
  qlever::KeyOrder::Array inverseKeys{};
  for (uint8_t i = 0; i < 4; ++i) {
    inverseKeys[perm.keyOrder().keys()[i]] = i;
  }

  const auto& reader = perm.reader();
  const auto& blockMetadata = perm.metaData().blockData();
  AD_CORRECTNESS_CHECK(!blockMetadata.empty());

  for (size_t blockIndex : blocksToVacuum(map_)) {
    AD_CORRECTNESS_CHECK(blockIndex <= blockMetadata.size());
    // This is one past the last block with index triples. This block always
    // only has updates but no index triples. Pass in an empty `IdTable`.
    if (blockIndex == blockMetadata.size()) {
      ad_utility::AllocatorWithLimit<Id> allocator =
          ad_utility::makeAllocatorWithLimit<Id>(0_B);
      IdTable idTable(4, allocator);
      totalStats +=
          processBlockForVacuum(idTable, map_.at(blockIndex), inverseKeys,
                                allDeletionsToRemove, allInsertionsToRemove);
      continue;
    }

    auto idTable = reader.readBlockWithoutLocatedTriples(
        blockMetadata.at(blockIndex),
        std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID});

    totalStats +=
        processBlockForVacuum(idTable, map_.at(blockIndex), inverseKeys,
                              allDeletionsToRemove, allInsertionsToRemove);
    cancellationHandle->throwIfCancelled();
  }

  return {std::move(allDeletionsToRemove), std::move(allInsertionsToRemove),
          totalStats};
}

// ____________________________________________________________________________
RowsToVacuum LocatedTriplesPerBlock::identifyRowsToVacuum(
    const Permutation& perm,
    ad_utility::SharedCancellationHandle cancellationHandle) const {
  RowsToVacuum result{{}, {}, {0, 0, 0, 0}};
  size_t numInsertions = 0;
  size_t numDeletions = 0;
  const size_t numColumns = 4 + numPayloadColumns_;
  // Read the graph column and all payload columns.
  std::vector<ColumnIndex> additionalColumns;
  for (ColumnIndex col = ADDITIONAL_COLUMN_GRAPH_ID; col < numColumns; ++col) {
    additionalColumns.push_back(col);
  }
  const auto& blockMetadata = perm.metaData().blockData();
  for (size_t blockIndex : blocksToVacuum(map_)) {
    AD_CORRECTNESS_CHECK(blockIndex <= blockMetadata.size());
    // The block one past the last block only has updates, see
    // `identifyTriplesToVacuum`.
    IdTable block =
        blockIndex == blockMetadata.size()
            ? IdTable{numColumns, ad_utility::makeAllocatorWithLimit<Id>(0_B)}
            : perm.reader().readBlockWithoutLocatedTriples(
                  blockMetadata.at(blockIndex), additionalColumns);
    // Both the located triples and the rows of the block are sorted by the
    // full row and unique (the view is updatable).
    auto rowIt = block.begin();
    for (const LocatedTriple& lt : map_.at(blockIndex).getSortedView()) {
      while (rowIt != block.end() && compareFullRow(lt, *rowIt) > 0) {
        ++rowIt;
      }
      bool isInView = rowIt != block.end() && compareFullRow(lt, *rowIt) == 0;
      (lt.insertOrDelete_ ? numInsertions : numDeletions)++;
      // Inserting a row that is in the view or deleting a row that is not in
      // the view has no effect.
      if (lt.insertOrDelete_ == isInView) {
        (lt.insertOrDelete_ ? result.insertionsToRemove_
                            : result.deletionsToRemove_)
            .push_back(lt);
      }
    }
    cancellationHandle->throwIfCancelled();
  }
  auto& stats = result.stats_;
  stats.numInsertionsRemoved_ = result.insertionsToRemove_.size();
  stats.numDeletionsRemoved_ = result.deletionsToRemove_.size();
  stats.numInsertionsKept_ = numInsertions - stats.numInsertionsRemoved_;
  stats.numDeletionsKept_ = numDeletions - stats.numDeletionsRemoved_;
  return result;
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::add(ql::span<const LocatedTriple> locatedTriples,
                                 ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("adding");
  for (const auto& locatedTriple : locatedTriples) {
    AD_CONTRACT_CHECK(locatedTriple.payload_.size() == numPayloadColumns_);
    map_[locatedTriple.blockIndex_].insert(locatedTriple);
  }
  tracer.endTrace("adding");
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::erase(size_t blockIndex, const LocatedTriple& lt) {
  auto blockIter = map_.find(blockIndex);
  AD_CONTRACT_CHECK(blockIter != map_.end(), "Block ", blockIndex,
                    " is not contained");
  auto& block = blockIter->second;
  block.erase(lt);
  if (block.empty()) {
    map_.erase(blockIndex);
  }
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::erase(ql::span<LocatedTriple> sortedTriples) {
  AD_CORRECTNESS_CHECK(
      ql::ranges::is_sorted(sortedTriples, {}, LocatedTriplesProjection{}));

  for (const auto chunk :
       ::ranges::views::chunk_by(sortedTriples, [](auto& lt1, auto& lt2) {
         return lt1.blockIndex_ == lt2.blockIndex_;
       })) {
    size_t blockIndex = chunk.front().blockIndex_;
    auto blockIter = map_.find(blockIndex);
    AD_CONTRACT_CHECK(blockIter != map_.end(), "Block ", blockIndex,
                      " is not contained");
    auto& block = blockIter->second;
    block.eraseSorted(chunk);
    if (block.empty()) {
      map_.erase(blockIndex);
    }
  }
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::numTriplesForTesting() const {
  return ::ranges::accumulate(
      map_ | ql::views::values |
          ql::views::transform(&LocatedTriples::sizeForTesting),
      size_t{0});
}

// ____________________________________________________________________________
bool LocatedTriplesPerBlock::containsLocatedTriplesInBlockRange(
    size_t firstBlockIndex, size_t lastBlockIndex) const {
  AD_CONTRACT_CHECK(firstBlockIndex <= lastBlockIndex);
  if (map_.size() <= lastBlockIndex - firstBlockIndex) {
    return ql::ranges::any_of(map_ | ql::views::keys, [&](size_t blockIndex) {
      return blockIndex >= firstBlockIndex && blockIndex <= lastBlockIndex;
    });
  }
  return ql::ranges::any_of(
      ql::views::iota(firstBlockIndex, lastBlockIndex + 1),
      [this](size_t blockIndex) { return map_.contains(blockIndex); });
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::setOriginalMetadata(
    std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata) {
  originalMetadata_ = std::move(metadata);
}

// Update the `blockMetadata`, such that its graph info is consistent with the
// `locatedTriples` which are added to that block. In particular, all graphs to
// which at least one triple is inserted become part of the graph info, and if
// the number of total graphs becomes larger than the configured threshold, then
// the graph info is set to `nullopt`, which means that there is no info.
void updateGraphMetadata(CompressedBlockMetadata& blockMetadata,
                         const LocatedTriples& locatedTriples) {
  auto& graphs = blockMetadata.graphInfo_;
  // We only insert graphs, never delete them, so if `graphs` is already
  // `nullopt`, then it will stay `nullopt`.
  if (graphs.has_value()) {
    graphs = computeDistinctGraphs(
        locatedTriples.getSortedView() |
            ql::views::filter(&LocatedTriple::insertOrDelete_) |
            ql::views::transform([](const LocatedTriple& lt) {
              return lt.triple_.ids().at(ADDITIONAL_COLUMN_GRAPH_ID);
            }),
        graphs.value());
  }

  if (!hasOnlyOneGraph(graphs)) {
    // We do not know anything about the triples contained in the block, so we
    // also cannot know if the `locatedTriples` introduces duplicates. We thus
    // have to be conservative and assume that there are duplicates when data
    // was inserted.
    blockMetadata.containsDuplicatesWithDifferentGraphs_ |= ql::ranges::any_of(
        locatedTriples.getSortedView(), &LocatedTriple::insertOrDelete_);
  }
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::updateAugmentedMetadata() {
  // TODO<C++23> use view::enumerate
  size_t blockIndex = 0;
  // Copy to preserve originalMetadata_.
  if (!originalMetadata_.has_value()) {
    AD_LOG_WARN << "The original metadata has not been set, but updates are "
                   "being performed. This should only happen in unit tests\n";
    augmentedMetadata_.emplace();
  } else {
    augmentedMetadata_ = *originalMetadata_.value();
  }
  for (auto& blockMetadata : augmentedMetadata_.value()) {
    if (auto blockUpdates = getUpdatesIfPresent(blockIndex)) {
      blockMetadata.firstTriple_ =
          std::min(blockMetadata.firstTriple_,
                   blockUpdates->front().triple_.toPermutedTriple());
      blockMetadata.lastTriple_ =
          std::max(blockMetadata.lastTriple_,
                   blockUpdates->back().triple_.toPermutedTriple());
      updateGraphMetadata(blockMetadata, *blockUpdates);
    }
    blockIndex++;
  }
  // Also account for the last block that contains the triples that are larger
  // than all the inserted triples.
  if (auto blockUpdates = getUpdatesIfPresent(blockIndex)) {
    auto firstTriple = blockUpdates->front().triple_.toPermutedTriple();
    auto lastTriple = blockUpdates->back().triple_.toPermutedTriple();

    // The first `std::nullopt` means that this block contains only
    // `LocatedTriple`s.
    CompressedBlockMetadataNoBlockIndex lastBlockN{
        std::nullopt, 0, firstTriple, lastTriple, std::nullopt, true};
    lastBlockN.graphInfo_.emplace();
    CompressedBlockMetadata lastBlock{lastBlockN, blockIndex};
    updateGraphMetadata(lastBlock, *blockUpdates);
    augmentedMetadata_->push_back(lastBlock);

    AD_CORRECTNESS_CHECK(
        CompressedBlockMetadata::checkInvariantsForSortedBlocks(
            *augmentedMetadata_));
  }
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const VacuumStatistics& stats) {
  j = nlohmann::json{{"insertionsRemoved", stats.numInsertionsRemoved_},
                     {"deletionsRemoved", stats.numDeletionsRemoved_},
                     {"insertionsKept", stats.numInsertionsKept_},
                     {"deletionsKept", stats.numDeletionsKept_},
                     {"totalRemoved", stats.totalRemoved()},
                     {"totalKept", stats.totalKept()}};
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const std::vector<IdTriple<0>>& v) {
  ql::ranges::copy(v, std::ostream_iterator<IdTriple<0>>(os, ", "));
  return os;
}

// ____________________________________________________________________________
bool LocatedTriplesPerBlock::isLocatedTriple(
    const IdTriple<0>& triple, bool insertOrDelete,
    const std::vector<Id>& payload) const {
  auto blockContains = [&triple, insertOrDelete, &payload](
                           const LocatedTriples& lt, size_t blockIndex) {
    LocatedTriple locatedTriple{blockIndex, triple, payload, insertOrDelete};
    locatedTriple.blockIndex_ = blockIndex;
    return ad_utility::contains(lt.getSortedView(), locatedTriple);
  };

  return ql::ranges::any_of(map_, [&blockContains](auto& indexAndBlock) {
    const auto& [index, block] = indexAndBlock;
    return blockContains(block, index);
  });
}

// _____________________________________________________________________________
std::array<std::vector<IdTriple<0>>, 2> LocatedTriplesPerBlock::computeDiff(
    const LocatedTriplesPerBlock& oldBlocks) const {
  // The result consists of the `triple_`s only, so a `payload_` would be lost.
  // This is fine because `computeDiff` is only used for the main index (to
  // carry over the updates when rebuilding the index, which doesn't carry over
  // the materialized views).
  AD_CONTRACT_CHECK(!mergesFullRows_ && !oldBlocks.mergesFullRows_);
  std::array<std::vector<IdTriple<0>>, 2> result;
  auto addTriple = [&result](const LocatedTriple& lt) {
    result.at(lt.insertOrDelete_ ? 0 : 1).push_back(lt.triple_);
  };

  // Compute all `LocatedTriples` that are in the new snapshot, but not in the
  // old snapshot requires comparing by the `IdTriple` but also
  // `insertOrDelete_`. Such triples have been newly inserted, newly deleted or
  // changed (from inserted to deleted or vice versa) since the old snapshot.
  for (const auto& [blockIndex, currentTriples] : map_) {
    auto it = oldBlocks.map_.find(blockIndex);
    const LocatedTriples empty;
    const auto& oldTriplesSortedView = it != oldBlocks.map_.end()
                                           ? it->second.getSortedView()
                                           : empty.getSortedView();
    // The default comparator compares the whole `LocatedTriple` with
    // `IdTriple`, `insertOrDelete_` and `blockIndex_`. When the `IdTriple`s are
    // equal the `blockIndex_` is also the same, so this does the right thing.
    ql::ranges::set_difference(
        currentTriples.getSortedView(), oldTriplesSortedView,
        ad_utility::IteratorForAssigmentOperator(addTriple));
  }
  // Account for non-deterministic order introduced by hash map. (Or in case a
  // permutation that is not SPO was used).
  ql::ranges::for_each(result, ql::ranges::sort);

  return result;
}
