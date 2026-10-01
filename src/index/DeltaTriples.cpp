// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2024 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "index/DeltaTriples.h"

#include <absl/strings/str_cat.h>

#include "Permutation.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/ExecuteUpdate.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "global/MaterializedViewConstants.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/IndexRebuilder.h"
#include "index/LocatedTriples.h"
#include "index/TripleComponentConversions.h"
#include "util/ChunkedForLoop.h"
#include "util/Serializer/TripleSerializer.h"

// ____________________________________________________________________________
template <bool isInternal>
const LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation(
    Permutation::Enum permutation) const {
  if constexpr (isInternal) {
    AD_CONTRACT_CHECK(permutation == Permutation::PSO ||
                      permutation == Permutation::POS);
  }
  return getLocatedTriples<isInternal>().at(static_cast<int>(permutation));
}

template const LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<true>(
    Permutation::Enum permutation) const;
template const LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<false>(
    Permutation::Enum permutation) const;

// ____________________________________________________________________________
template <bool isInternal>
LocatedTriplesPerBlock& LocatedTriplesState::getLocatedTriplesForPermutation(
    Permutation::Enum permutation) {
  return const_cast<LocatedTriplesPerBlock&>(
      std::as_const(*this).getLocatedTriplesForPermutation<isInternal>(
          permutation));
}

template LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<true>(
    Permutation::Enum permutation);
template LocatedTriplesPerBlock&
LocatedTriplesState::getLocatedTriplesForPermutation<false>(
    Permutation::Enum permutation);

// ____________________________________________________________________________
template <bool isInternal>
LocatedTriplesPerBlockAllPermutations<isInternal>&
LocatedTriplesState::getLocatedTriples() {
  return const_cast<LocatedTriplesPerBlockAllPermutations<isInternal>&>(
      std::as_const(*this).getLocatedTriples<isInternal>());
}

// ____________________________________________________________________________
template <bool isInternal>
const LocatedTriplesPerBlockAllPermutations<isInternal>&
LocatedTriplesState::getLocatedTriples() const {
  if constexpr (isInternal) {
    return internalLocatedTriplesPerBlock_;
  } else {
    return locatedTriplesPerBlock_;
  }
}

// ____________________________________________________________________________
void DeltaTriples::clear() {
  auto clearImpl = [](auto& state, auto& locatedTriples) {
    state.triplesInserted_.clear();
    state.triplesDeleted_.clear();
    ql::ranges::for_each(locatedTriples, &LocatedTriplesPerBlock::clear);
  };
  clearImpl(triplesSetsNormal_, locatedTriples_->getLocatedTriples<false>());
  clearImpl(triplesSetsInternal_, locatedTriples_->getLocatedTriples<true>());
  // The views stay registered, only their updates are dropped.
  for (auto& [name, view] : views_) {
    view.rowsInserted_.clear();
    view.rowsDeleted_.clear();
    viewLocatedRows(name).clear();
  }
}

// ____________________________________________________________________________
template <typename IsInternal>
void DeltaTriples::eraseTriplesInPermutation(
    Permutation::Enum permutation, ql::span<const IdTriple<0>> triples,
    IsInternal isInternal,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  // The requested `Permutation` and `LocatedTriplesPerBlock` for that
  // permutation from which the triples are erased.
  const auto& perm = index_.getPermutation(permutation);
  LocatedTriplesPerBlock& lts =
      locatedTriples_->getLocatedTriplesForPermutation<isInternal>(permutation);
  // Determine the `blockIndex` for each of the `triples` which is required for
  // deletion.
  // Note: The value for `insertOrDelete` is never used, because the `erase`
  // interface erases all matching triples, independent of that flag. We still
  // need to pass a value for it though as the erase API reuses the
  // `LocatedTriple` type which always needs this flag.
  static constexpr bool insertOrDeleteDummyValue = false;
  auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
      triples, perm.metaData().blockData(), perm.keyOrder(),
      insertOrDeleteDummyValue, cancellationHandle);
  // `LocatedTriplesPerBlock::erase` requires a sorted input.
  ql::ranges::sort(locatedTriples, {}, LocatedTriplesProjection{});
  lts.erase(locatedTriples);
}

// ____________________________________________________________________________
nlohmann::json DeltaTriples::vacuum(
    ad_utility::SharedCancellationHandle cancellationHandle) {
  // When the cancellation handle stops the execution this results in the state
  // that only a part of the triples have been vacuumed, which is valid.
  using namespace ad_utility::use_value_identity;

  // Remove the `triples` from all the permutations.
  auto removeFromAllPermutations =
      [this](const std::vector<IdTriple<0>>& triples, auto& triplesToHandlesMap,
             auto isInternal) {
        // This operation must not be interrupted so we ignore the
        // CancellationHandle.
        auto cancellationHandle =
            std::make_shared<ad_utility::CancellationHandle<>>();
        // Erase located triples
        for (auto permutation : Permutation::all<isInternal>()) {
          eraseTriplesInPermutation(permutation, triples, isInternal,
                                    cancellationHandle);
        }

        // Remove from handles map.
        for (const auto& triple : triples) {
          AD_CORRECTNESS_CHECK(triplesToHandlesMap.erase(triple) == 1);
        }
      };
  auto identifyTriplesToVacuum = [this, &cancellationHandle,
                                  &removeFromAllPermutations](auto isInternal) {
    auto perm = Permutation::PSO;
    auto& basePerm = index_.getPermutation(perm);
    const auto& actualPerm =
        isInternal ? basePerm.internalPermutation() : basePerm;
    const auto& ltpb =
        locatedTriples_->getLocatedTriplesForPermutation<isInternal>(perm);
    auto [deletions, insertions, stats] =
        ltpb.identifyTriplesToVacuum(actualPerm, cancellationHandle);
    return std::make_pair(
        [isInternal, this, &removeFromAllPermutations,
         deletions = std::move(deletions),
         insertions = std::move(insertions)]() {
          auto& state = getState<isInternal>();
          removeFromAllPermutations(deletions, state.triplesDeleted_,
                                    isInternal);
          removeFromAllPermutations(insertions, state.triplesInserted_,
                                    isInternal);
        },
        stats);
  };

  nlohmann::json result = nlohmann::json::object();
  auto [removeExternal, externalStats] = identifyTriplesToVacuum(vi<false>);
  auto [removeInternal, internalStats] = identifyTriplesToVacuum(vi<true>);
  std::vector<std::pair<std::string, RowsToVacuum>> viewRowsToVacuum;
  for (const auto& [name, view] : views_) {
    viewRowsToVacuum.emplace_back(
        name,
        locatedTriples_->viewLocatedTriples_.at(name).identifyRowsToVacuum(
            *view.permutation_, cancellationHandle));
  }
  // Remove the located `rows` from the view with the given `name` and from its
  // `rowSet` (`rowsInserted_` or `rowsDeleted_`).
  auto removeViewRows = [this](const std::string& name,
                               std::vector<LocatedTriple>& rows,
                               ViewState::RowSet& rowSet) {
    // `LocatedTriplesPerBlock::erase` requires a sorted input.
    ql::ranges::sort(rows, {}, LocatedTriplesProjection{});
    locatedTriples_->viewLocatedTriples_.at(name).erase(rows);
    for (const auto& lt : rows) {
      std::vector<Id> row{lt.triple_.ids().begin(), lt.triple_.ids().end()};
      row.insert(row.end(), lt.payload_.begin(), lt.payload_.end());
      AD_CORRECTNESS_CHECK(rowSet.erase(row) == 1);
    }
  };

  // For a consistent state this block must be executed fully.
  // CancellationHandle's must be ignored inside this block.
  {
    removeExternal();
    result["external"] = externalStats;

    removeInternal();
    result["internal"] = internalStats;

    result["views"] = nlohmann::json::object();
    for (auto& [name, rowsToVacuum] : viewRowsToVacuum) {
      auto& view = views_.at(name);
      removeViewRows(name, rowsToVacuum.deletionsToRemove_, view.rowsDeleted_);
      removeViewRows(name, rowsToVacuum.insertionsToRemove_,
                     view.rowsInserted_);
      result["views"][name] = rowsToVacuum.stats_;
    }
  }

  return result;
}

// ____________________________________________________________________________
template <bool isInternal>
DeltaTriples::TriplesSets<isInternal>& DeltaTriples::getState() {
  if constexpr (isInternal) {
    return triplesSetsInternal_;
  } else {
    return triplesSetsNormal_;
  }
}

// ____________________________________________________________________________
template <bool isInternal>
void DeltaTriples::locateAndAddTriples(CancellationHandle cancellationHandle,
                                       ql::span<const IdTriple<0>> triples,
                                       bool insertOrDelete,
                                       ad_utility::timer::TimeTracer& tracer) {
  constexpr const auto& allPermutations = Permutation::all<isInternal>();
  auto& lt = locatedTriples_->getLocatedTriples<isInternal>();
  for (auto permutation : allPermutations) {
    tracer.beginTrace(std::string{Permutation::toString(permutation)});
    tracer.beginTrace("locateTriples");
    auto& basePerm = index_.getPermutation(permutation);
    auto& perm = isInternal ? basePerm.internalPermutation() : basePerm;
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        triples, perm.metaData().blockData(), perm.keyOrder(), insertOrDelete,
        cancellationHandle);
    cancellationHandle->throwIfCancelled();
    tracer.endTrace("locateTriples");
    tracer.beginTrace("addToLocatedTriples");
    lt[static_cast<size_t>(permutation)].add(locatedTriples, tracer);
    cancellationHandle->throwIfCancelled();
    tracer.endTrace("addToLocatedTriples");
    tracer.endTrace(Permutation::toString(permutation));
  }
}

// ____________________________________________________________________________
DeltaTriplesCount DeltaTriples::getCounts() const {
  return {numInserted(), numDeleted()};
}

// _____________________________________________________________________________
DeltaTriples::Triples DeltaTriples::makeInternalTriples(const Triples& triples,
                                                        bool insertion) {
  // NOTE: If this logic is ever changed, you need to also change the code
  // in `IndexBuilderTypes.h`, the function `mapTripleToIds` specifically,
  // which adds the same extra triples for language tags to the internal triples
  // on the initial index build.
  Triples internalTriples;
  // Initialize on first use.
  if (languagePredicate_.isUndefined()) {
    languagePredicate_ = toValueId(
        TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(LANGUAGE_PREDICATE)},
        index_, localVocab_);
  }
  ad_utility::HashSet<Id> addedObjects;
  for (const auto& triple : triples) {
    const auto& ids = triple.ids();
    Id objectId = ids.at(2);
    auto optionalLiteralOrIri =
        ql::exportIds::idToLiteralOrIri(index_, objectId, localVocab_, true);
    if (!optionalLiteralOrIri.has_value() ||
        !optionalLiteralOrIri.value().isLiteral() ||
        !optionalLiteralOrIri.value().hasLanguageTag()) {
      continue;
    }
    const auto& predicate =
        predicateCache_.getOrCompute(ids.at(1).getBits(), [this](Id::T bits) {
          auto optionalPredicate = ql::exportIds::idToLiteralOrIri(
              index_, Id::fromBits(bits), localVocab_, true);
          AD_CORRECTNESS_CHECK(optionalPredicate.has_value());
          AD_CORRECTNESS_CHECK(optionalPredicate.value().isIri());
          return std::move(optionalPredicate.value().getIri());
        });
    auto langtag =
        asStringViewUnsafe(optionalLiteralOrIri.value().getLanguageTag());
    auto specialPredicate = predicate.withLanguageTag(langtag);
    Id specialId = toValueId(TripleComponent{std::move(specialPredicate)},
                             index_, localVocab_);
    // Extra triple `<subject> @language@<predicate> "object"@language`.
    internalTriples.push_back(
        IdTriple<0>{std::array{ids.at(0), specialId, objectId, ids.at(3)}});
    // If we have already added the triple for this object with its langtag we
    // can't add it a second time.
    if (addedObjects.contains(objectId)) {
      continue;
    }
    Id langtagId =
        languageTagCache_.getOrCompute(langtag, [this](const std::string& tag) {
          return toValueId(
              TripleComponent{
                  ad_utility::triple_component::Iri::fromLangtag(tag)},
              index_, localVocab_);
        });

    // Because we don't track the exact counts of existing objects, we just
    // conservatively add these internal triples on insertion, and never remove
    // them. This is inefficient, but never wrong because queries that use these
    // internal triples will always join these internal triples with a regular
    // index scan.
    if (insertion) {
      // Extra triple `"object"@language ql:langtag <@language>`.
      internalTriples.push_back(IdTriple<0>{
          std::array{objectId, languagePredicate_, langtagId, ids.at(3)}});
      addedObjects.emplace(objectId);
    }
  }
  // Because of the special predicates, we need to re-sort the triples.
  ql::ranges::sort(internalTriples);
  return internalTriples;
}

// ____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::insertTriples(CancellationHandle cancellationHandle,
                                 Triples triples,
                                 ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("makeInternalTriples");
  auto internalTriples = makeInternalTriples(triples, true);
  tracer.endTrace("makeInternalTriples");
  tracer.beginTrace("externalPermutation");
  modifyTriplesImpl<false, true>(cancellationHandle, std::move(triples),
                                 tracer);
  tracer.endTrace("externalPermutation");
  tracer.beginTrace("internalPermutation");
  modifyTriplesImpl<true, true>(std::move(cancellationHandle),
                                std::move(internalTriples), tracer);
  tracer.endTrace("internalPermutation");
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
  consolidateIfRequested<consolidate>(tracer);
}
template void DeltaTriples::insertTriples<DeltaTriples::Consolidate::Yes>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);
template void DeltaTriples::insertTriples<DeltaTriples::Consolidate::No>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);

// ____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::deleteTriples(CancellationHandle cancellationHandle,
                                 Triples triples,
                                 ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("makeInternalTriples");
  auto internalTriples = makeInternalTriples(triples, false);
  tracer.endTrace("makeInternalTriples");
  tracer.beginTrace("externalPermutation");
  modifyTriplesImpl<false, false>(cancellationHandle, std::move(triples),
                                  tracer);
  tracer.endTrace("externalPermutation");
  tracer.beginTrace("internalPermutation");
  modifyTriplesImpl<true, false>(std::move(cancellationHandle),
                                 std::move(internalTriples), tracer);
  tracer.endTrace("internalPermutation");
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
  consolidateIfRequested<consolidate>(tracer);
}
template void DeltaTriples::deleteTriples<DeltaTriples::Consolidate::Yes>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);
template void DeltaTriples::deleteTriples<DeltaTriples::Consolidate::No>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);

// ____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::insertInternalTriplesForTesting(
    CancellationHandle cancellationHandle, Triples triples,
    ad_utility::timer::TimeTracer& tracer) {
  modifyTriplesImpl<true, true>(std::move(cancellationHandle),
                                std::move(triples), tracer);
  consolidateIfRequested<consolidate>(tracer);
}
template void
DeltaTriples::insertInternalTriplesForTesting<DeltaTriples::Consolidate::Yes>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);
template void
DeltaTriples::insertInternalTriplesForTesting<DeltaTriples::Consolidate::No>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);

// ____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::deleteInternalTriplesForTesting(
    CancellationHandle cancellationHandle, Triples triples,
    ad_utility::timer::TimeTracer& tracer) {
  modifyTriplesImpl<true, false>(std::move(cancellationHandle),
                                 std::move(triples), tracer);
  consolidateIfRequested<consolidate>(tracer);
}
template void
DeltaTriples::deleteInternalTriplesForTesting<DeltaTriples::Consolidate::Yes>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);
template void
DeltaTriples::deleteInternalTriplesForTesting<DeltaTriples::Consolidate::No>(
    CancellationHandle, Triples, ad_utility::timer::TimeTracer&);

namespace {
// A cheap identity of a materialized view with the given block `metadata` and
// `numColumns`: the number of columns, blocks, and rows, and the first and last
// triple. Two different versions of a view (written with the same name) are
// very unlikely to have the same identity.
std::vector<Id> viewIdentity(
    const std::vector<CompressedBlockMetadata>& metadata, size_t numColumns) {
  auto I = [](size_t value) {
    return Id::makeFromInt(static_cast<int64_t>(value));
  };
  size_t numRows = 0;
  for (const auto& block : metadata) {
    numRows += block.numRows_;
  }
  std::vector<Id> identity{I(numColumns), I(metadata.size()), I(numRows)};
  if (!metadata.empty()) {
    for (const auto& t :
         {metadata.front().firstTriple_, metadata.back().lastTriple_}) {
      identity.insert(identity.end(),
                      {t.col0Id_, t.col1Id_, t.col2Id_, t.graphId_});
    }
  }
  return identity;
}
}  // namespace

// ____________________________________________________________________________
void DeltaTriples::registerView(
    const std::string& name, std::shared_ptr<const Permutation> permutation,
    size_t numColumns,
    ad_utility::HashSet<ColumnIndex> possiblyUndefinedColumns) {
  AD_CONTRACT_CHECK(permutation != nullptr);
  auto metadata = permutation->metaData().blockDataShared();
  AD_CONTRACT_CHECK(metadata != nullptr);
  if (auto it = views_.find(name);
      it != views_.end() &&
      viewLocatedRows(name).hasOriginalMetadata(*metadata)) {
    AD_CONTRACT_CHECK(it->second.numColumns_ == numColumns);
    return;
  }
  // A replaced registration may have had updates, see `unregisterView`.
  unregisterView(name);
  auto identity = viewIdentity(*metadata, numColumns);
  // Views with less than four columns are stored padded to four columns.
  LocatedTriplesPerBlock locatedRows;
  locatedRows.setNumPayloadColumns(std::max(numColumns, size_t{4}) - 4);
  locatedRows.setOriginalMetadata(std::move(metadata));
  locatedTriples_->viewLocatedTriples_.emplace(name, std::move(locatedRows));
  views_.emplace(name, ViewState{numColumns,
                                 std::move(permutation),
                                 std::move(identity),
                                 std::move(possiblyUndefinedColumns),
                                 {},
                                 {}});
  readViewFromDisk(name);
}

// ____________________________________________________________________________
void DeltaTriples::readViewFromDisk(const std::string& name) {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  auto filename = viewFilename(name);
  auto [vocab, idRanges] =
      ad_utility::deserializeIds(filename, index_.getLocalVocabContext());
  if (idRanges.empty()) {
    return;
  }
  AD_CORRECTNESS_CHECK(idRanges.size() == 3);
  if (idRanges.at(2) != views_.at(name).identity_) {
    AD_LOG_WARN << "The persisted updates of the materialized view \"" << name
                << "\" belong to a different version of the view and are "
                   "discarded"
                << std::endl;
    ql::filesystem::remove(filename);
    return;
  }
  auto& locatedRows = locatedTriples_->viewLocatedTriples_.at(name);
  size_t numColumns = 4 + locatedRows.numPayloadColumns();
  auto toRows = [numColumns](const std::vector<Id>& ids) {
    AD_CORRECTNESS_CHECK(ids.size() % numColumns == 0);
    IdTable rows{numColumns, ad_utility::makeUnlimitedAllocator<Id>()};
    rows.resize(ids.size() / numColumns);
    for (size_t i = 0; i < rows.numRows(); ++i) {
      for (size_t col = 0; col < numColumns; ++col) {
        rows(i, col) = ids[i * numColumns + col];
      }
    }
    return rows;
  };
  auto cancellationHandle =
      std::make_shared<CancellationHandle::element_type>();
  insertViewRows<Consolidate::No>(cancellationHandle, name,
                                  toRows(idRanges.at(1)));
  deleteViewRows<Consolidate::No>(cancellationHandle, name,
                                  toRows(idRanges.at(0)));
  locatedRows.consolidateAllBlocks();
  // The registration doesn't update the metadata (see
  // `MaterializedViewsManager::syncViewRegistration`), so do it here.
  locatedRows.updateAugmentedMetadata();
  AD_LOG_INFO << "Done, #inserted rows = " << idRanges.at(1).size() / numColumns
              << ", #deleted rows = " << idRanges.at(0).size() / numColumns
              << std::endl;
}

// ____________________________________________________________________________
std::string DeltaTriples::viewFilename(const std::string& name) const {
  // NOTE: The `name` is safe to use in a filename, because the names of
  // materialized views are checked by `MaterializedView::throwIfInvalidName`.
  return absl::StrCat(filenameForPersisting_.value(), VIEW_FILE_INFIX, name);
}

// ____________________________________________________________________________
LocatedTriplesPerBlock& DeltaTriples::viewLocatedRows(const std::string& name) {
  auto it = locatedTriples_->viewLocatedTriples_.find(name);
  // `views_` and `viewLocatedTriples_` always have the same keys.
  AD_CORRECTNESS_CHECK(it != locatedTriples_->viewLocatedTriples_.end());
  return it->second;
}

// ____________________________________________________________________________
void DeltaTriples::unregisterView(const std::string& name) {
  auto it = locatedTriples_->viewLocatedTriples_.find(name);
  if (it == locatedTriples_->viewLocatedTriples_.end()) {
    return;
  }
  // Only if updates are dropped, the content of the view changes. Otherwise
  // don't touch the `index_` to not needlessly invalidate the query cache.
  if (!it->second.isEmpty()) {
    locatedTriples_->index_++;
  }
  if (filenameForPersisting_.has_value()) {
    ql::filesystem::remove(viewFilename(name));
  }
  locatedTriples_->viewLocatedTriples_.erase(it);
  views_.erase(name);
}

// ____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::insertViewRows(CancellationHandle cancellationHandle,
                                  const std::string& name, IdTable rows,
                                  ad_utility::timer::TimeTracer& tracer) {
  modifyViewRowsImpl<true>(std::move(cancellationHandle), name, std::move(rows),
                           tracer);
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
  consolidateIfRequested<consolidate>(tracer);
}
template void DeltaTriples::insertViewRows<DeltaTriples::Consolidate::Yes>(
    CancellationHandle, const std::string&, IdTable,
    ad_utility::timer::TimeTracer&);
template void DeltaTriples::insertViewRows<DeltaTriples::Consolidate::No>(
    CancellationHandle, const std::string&, IdTable,
    ad_utility::timer::TimeTracer&);

// ____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::deleteViewRows(CancellationHandle cancellationHandle,
                                  const std::string& name, IdTable rows,
                                  ad_utility::timer::TimeTracer& tracer) {
  modifyViewRowsImpl<false>(std::move(cancellationHandle), name,
                            std::move(rows), tracer);
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
  consolidateIfRequested<consolidate>(tracer);
}
template void DeltaTriples::deleteViewRows<DeltaTriples::Consolidate::Yes>(
    CancellationHandle, const std::string&, IdTable,
    ad_utility::timer::TimeTracer&);
template void DeltaTriples::deleteViewRows<DeltaTriples::Consolidate::No>(
    CancellationHandle, const std::string&, IdTable,
    ad_utility::timer::TimeTracer&);

// ____________________________________________________________________________
template <bool insertOrDelete>
void DeltaTriples::modifyViewRowsImpl(CancellationHandle cancellationHandle,
                                      const std::string& name, IdTable rows,
                                      ad_utility::timer::TimeTracer& tracer) {
  auto it = views_.find(name);
  AD_CONTRACT_CHECK(it != views_.end(), [&name]() {
    return absl::StrCat("The materialized view '", name,
                        "' is not registered for updates.");
  });
  auto& view = it->second;
  auto& locatedRows = viewLocatedRows(name);
  AD_CONTRACT_CHECK(rows.numColumns() == 4 + locatedRows.numPayloadColumns(),
                    "The number of columns of the rows to be inserted into or "
                    "deleted from a materialized view must match the view.");
  for (size_t col = 0; col < rows.numColumns(); ++col) {
    // The padding columns of views with less than four columns are always
    // UNDEF.
    if (col >= view.numColumns_) {
      AD_CONTRACT_CHECK(
          ql::ranges::all_of(rows.getColumn(col), &Id::isUndefined), [col]() {
            return absl::StrCat("Column ", col,
                                " is a padding column of a materialized view "
                                "and must only contain UNDEF values.");
          });
      continue;
    }
    AD_CONTRACT_CHECK(
        view.possiblyUndefinedColumns_.contains(col) ||
            ql::ranges::none_of(rows.getColumn(col), &Id::isUndefined),
        [col]() {
          return absl::StrCat("Column ", col,
                              " of a materialized view must not contain "
                              "UNDEF values.");
        });
  }
  auto [targetSet, inverseSet] = [&view]() {
    if constexpr (insertOrDelete) {
      return std::tie(view.rowsInserted_, view.rowsDeleted_);
    } else {
      return std::tie(view.rowsDeleted_, view.rowsInserted_);
    }
  }();
  tracer.beginTrace("rewriteLocalVocabEntries");
  rewriteLocalVocabEntriesAndBlankNodes(rows);
  tracer.endTrace("rewriteLocalVocabEntries");

  // Unlike for the triples, we can't expect the caller to sort the rows, so
  // sort them here. Then (in place) drop the duplicates and the rows that are
  // already in the `targetSet`, and remove the remaining rows from the
  // `inverseSet`.
  ql::ranges::sort(rows, [](const auto& a, const auto& b) {
    return ql::ranges::lexicographical_compare(a, b);
  });
  std::vector<Id> key(rows.numColumns());
  auto toKey = [&key](const auto& row) -> const std::vector<Id>& {
    ql::ranges::copy(row, key.begin());
    return key;
  };
  size_t numKept = 0;
  for (size_t i = 0; i < rows.numRows(); ++i) {
    if ((numKept > 0 && rows[numKept - 1] == rows[i]) ||
        targetSet.contains(toKey(rows[i]))) {
      continue;
    }
    inverseSet.erase(key);
    if (numKept != i) {
      rows[numKept] = rows[i];
    }
    ++numKept;
  }
  rows.resize(numKept);

  tracer.beginTrace("locatedAndAdd");
  auto locatedTriples =
      LocatedTriple::locateRowsInView(rows, locatedRows.getOriginalMetadata(),
                                      insertOrDelete, cancellationHandle);
  cancellationHandle->throwIfCancelled();
  locatedRows.add(locatedTriples, tracer);
  tracer.endTrace("locatedAndAdd");

  for (const auto& row : rows) {
    targetSet.insert(toKey(row));
  }
}

// ____________________________________________________________________________
void DeltaTriples::rewriteLocalVocabEntriesAndBlankNodes(Triples& triples) {
  rewriteLocalVocabEntriesAndBlankNodesImpl([&triples](const auto& convertId) {
    ql::ranges::for_each(triples, [&convertId](IdTriple<0>& triple) {
      ql::ranges::for_each(triple.ids(), convertId);
      ql::ranges::for_each(triple.payload(), convertId);
    });
  });
}

// ____________________________________________________________________________
void DeltaTriples::rewriteLocalVocabEntriesAndBlankNodes(IdTable& rows) {
  rewriteLocalVocabEntriesAndBlankNodesImpl([&rows](const auto& convertId) {
    for (size_t col = 0; col < rows.numColumns(); ++col) {
      ql::ranges::for_each(rows.getColumn(col), convertId);
    }
  });
}

// ____________________________________________________________________________
template <typename ForEachId>
void DeltaTriples::rewriteLocalVocabEntriesAndBlankNodesImpl(
    const ForEachId& forEachId) {
  // Remember which original blank node (from the parsing of an insert
  // operation) is mapped to which blank node managed by the `localVocab_` of
  // this class.
  ad_utility::HashMap<Id, Id> blankNodeMap;
  // For the given original blank node `id`, check if it has already been
  // mapped. If not, map it to a new blank node managed by the `localVocab_`
  // of this class. Either way, return the (already existing or newly created)
  // value.
  auto getLocalBlankNode = [this, &blankNodeMap](Id id) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::BlankNodeIndex);
    // The following code handles both cases (already mapped or not) with a
    // single lookup in the map. Note that the value of the `try_emplace` call
    // is irrelevant.
    auto [it, newElement] = blankNodeMap.try_emplace(id, Id::makeUndefined());
    if (newElement) {
      it->second = Id::makeFromBlankNodeIndex(
          localVocab_.getBlankNodeIndex(index_.getBlankNodeManager()));
    }
    return it->second;
  };

  // Return true iff `blankNodeIndex` is a blank node index from the original
  // index.
  auto isGlobalBlankNode = [minLocalBlankNode =
                                index_.getBlankNodeManager()->minIndex_](
                               BlankNodeIndex blankNodeIndex) {
    return blankNodeIndex.get() < minLocalBlankNode;
  };

  // Helper lambda that converts a single local vocab or blank node `id` as
  // described in the comment for this function. All other types are left
  // unchanged.
  auto convertId = [this, isGlobalBlankNode, &getLocalBlankNode](Id& id) {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      // NOTE: `getIdAndAddIfNotContained` (not `getIndexAndAddIfNotContained`)
      // is essential here: storing a word that is already in the vocabulary of
      // the index would give the delta triples two different `Id`s for it, and
      // a later index rebuild would try to add it to the vocabulary again.
      id = localVocab_.getIdAndAddIfNotContained(*id.getLocalVocabIndex());
    } else if (id.getDatatype() == Datatype::BlankNodeIndex) {
      auto idx = id.getBlankNodeIndex();
      if (isGlobalBlankNode(idx) ||
          localVocab_.isBlankNodeIndexContained(idx)) {
        return;
      }
      id = getLocalBlankNode(id);
    }
  };

  // Convert all local vocab and blank node `Id`s.
  forEachId(convertId);
}

// ____________________________________________________________________________
template <bool isInternal, bool insertOrDelete>
void DeltaTriples::modifyTriplesImpl(CancellationHandle cancellationHandle,
                                     Triples triples,
                                     ad_utility::timer::TimeTracer& tracer) {
  AD_LOG_DEBUG << (insertOrDelete ? "Inserting" : "Deleting") << " "
               << triples.size() << (isInternal ? " internal" : "")
               << " triples (including idempotent triples)." << std::endl;
  auto [targetMap, inverseMap] = [this]() {
    auto& state = getState<isInternal>();
    if constexpr (insertOrDelete) {
      return std::tie(state.triplesInserted_, state.triplesDeleted_);
    } else {
      return std::tie(state.triplesDeleted_, state.triplesInserted_);
    }
  }();
  tracer.beginTrace("rewriteLocalVocabEntries");
  rewriteLocalVocabEntriesAndBlankNodes(triples);
  tracer.endTrace("rewriteLocalVocabEntries");
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(triples));
  AD_EXPENSIVE_CHECK(std::unique(triples.begin(), triples.end()) ==
                     triples.end());
  tracer.beginTrace("removeExistingTriples");
  ql::erase_if(triples, [&targetMap](const IdTriple<0>& triple) {
    return targetMap.contains(triple);
  });
  tracer.endTrace("removeExistingTriples");
  tracer.beginTrace("removeInverseTriples");
  ql::ranges::for_each(triples, [&inverseMap](const IdTriple<0>& triple) {
    // Note: if a triple does not exist, `erase` does nothing.
    inverseMap.erase(triple);
  });
  tracer.endTrace("removeInverseTriples");
  tracer.beginTrace("locatedAndAdd");

  locateAndAddTriples<isInternal>(std::move(cancellationHandle), triples,
                                  insertOrDelete, tracer);
  tracer.endTrace("locatedAndAdd");
  tracer.beginTrace("markTriples");

  ql::ranges::move(triples, std::inserter(targetMap, targetMap.end()));
  tracer.endTrace("markTriples");
}

// ____________________________________________________________________________
LocatedTriplesSharedState DeltaTriples::getLocatedTriplesSharedStateCopy()
    const {
  // Create a copy of the `LocatedTriplesState` for use as a constant
  // snapshot. NOTE: `LocatedTriplesState` is an aggregate, and `make_shared`
  // initializes with parentheses, which only works for aggregates since C++20.
  // The explicit `LocatedTriplesState{...}` is therefore required for C++17.
  return LocatedTriplesSharedState{
      std::make_shared<LocatedTriplesState>(LocatedTriplesState{
          locatedTriples_->locatedTriplesPerBlock_,
          locatedTriples_->internalLocatedTriplesPerBlock_,
          localVocab_.getLifetimeExtender(), locatedTriples_->index_,
          getCounts(), locatedTriples_->viewLocatedTriples_})};
}

// ____________________________________________________________________________
LocatedTriplesSharedState DeltaTriples::getLocatedTriplesSharedStateReference()
    const {
  // Creating a `shared_ptr<const LocatedTriplesState>` from a
  // `shared_ptr<LocatedTriplesState>` is cheap.
  return LocatedTriplesSharedState{locatedTriples_};
}

// ____________________________________________________________________________
DeltaTriples::DeltaTriples(const Index& index)
    : DeltaTriples(index.getImpl()) {}

// ____________________________________________________________________________
DeltaTriples::DeltaTriples(const IndexImpl& index) : index_{index} {}

// ____________________________________________________________________________
DeltaTriplesManager::DeltaTriplesManager(const IndexImpl& index)
    : deltaTriples_{index},
      currentLocatedTriplesSharedState_{
          deltaTriples_.wlock()->getLocatedTriplesSharedStateCopy()} {}

// _____________________________________________________________________________
template <typename ReturnType>
ReturnType DeltaTriplesManager::modify(
    const std::function<ReturnType(DeltaTriples&)>& function,
    bool writeToDiskAfterRequest, bool updateMetadataAfterRequest,
    ad_utility::timer::TimeTracer& tracer) {
  // While holding the lock for the underlying `DeltaTriples`, perform the
  // actual `function` (typically some combination of insert and delete
  // operations) and (while still holding the lock) update the
  // `currentLocatedTriplesSnapshot_`.
  tracer.beginTrace("acquiringDeltaTriplesWriteLock");
  return deltaTriples_.withWriteLock([this, &function, writeToDiskAfterRequest,
                                      updateMetadataAfterRequest,
                                      &tracer](DeltaTriples& deltaTriples) {
    auto updateSnapshot = [this, &deltaTriples] {
      auto newSnapshot = deltaTriples.getLocatedTriplesSharedStateCopy();
      currentLocatedTriplesSharedState_.withWriteLock(
          [&newSnapshot](auto& currentSnapshot) {
            currentSnapshot = std::move(newSnapshot);
          });
    };
    auto writeAndUpdateSnapshot = [&updateSnapshot, &deltaTriples, &tracer,
                                   writeToDiskAfterRequest]() {
      if (writeToDiskAfterRequest) {
        tracer.beginTrace("diskWriteback");
        deltaTriples.writeToDisk();
        tracer.endTrace("diskWriteback");
      }
      tracer.beginTrace("snapshotCreation");
      updateSnapshot();
      tracer.endTrace("snapshotCreation");
    };
    auto updateMetadata = [&tracer, &deltaTriples,
                           updateMetadataAfterRequest]() {
      if (updateMetadataAfterRequest) {
        tracer.beginTrace("metadataUpdateForSnapshot");
        deltaTriples.updateAugmentedMetadata();
        tracer.endTrace("metadataUpdateForSnapshot");
      }
    };

    tracer.endTrace("acquiringDeltaTriplesWriteLock");
    if constexpr (std::is_void_v<ReturnType>) {
      tracer.beginTrace("operations");
      function(deltaTriples);
      tracer.endTrace("operations");
      updateMetadata();
      writeAndUpdateSnapshot();
    } else {
      tracer.beginTrace("operations");
      ReturnType returnValue = function(deltaTriples);
      tracer.endTrace("operations");
      updateMetadata();
      writeAndUpdateSnapshot();
      return returnValue;
    }
  });
}
// Explicit instantiations
#define INSTANTIATE_MODIFY(T)                             \
  template T DeltaTriplesManager::modify<T>(              \
      const std::function<T(DeltaTriples&)>&, bool, bool, \
      ad_utility::timer::TimeTracer&)
INSTANTIATE_MODIFY(void);
INSTANTIATE_MODIFY(UpdateMetadata);
INSTANTIATE_MODIFY(DeltaTriplesCount);
INSTANTIATE_MODIFY(nlohmann::json);
#undef INSTANTIATE_MODIFY

// _____________________________________________________________________________
void DeltaTriplesManager::clear() { modify<void>(&DeltaTriples::clear); }

// _____________________________________________________________________________
LocatedTriplesSharedState
DeltaTriplesManager::getCurrentLocatedTriplesSharedState() const {
  return *currentLocatedTriplesSharedState_.rlock();
}

// _____________________________________________________________________________
std::tuple<
    LocatedTriplesSharedState, std::vector<LocalVocabIndex>,
    std::vector<
        ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>
DeltaTriplesManager::getCurrentLocatedTriplesSharedStateWithVocab() const {
  return deltaTriples_.withReadLock([this](const DeltaTriples& deltaTriples) {
    auto [indices, ownedBlocks] = deltaTriples.copyLocalVocab();
    return std::make_tuple(*currentLocatedTriplesSharedState_.rlock(),
                           std::move(indices), std::move(ownedBlocks));
  });
}

// _____________________________________________________________________________
void DeltaTriples::setOriginalMetadata(
    Permutation::Enum permutation,
    std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata,
    bool setInternalMetadata) {
  auto& locatedTriplesPerBlock =
      setInternalMetadata
          ? locatedTriples_->getLocatedTriplesForPermutation<true>(permutation)
          : locatedTriples_->getLocatedTriplesForPermutation<false>(
                permutation);
  locatedTriplesPerBlock.setOriginalMetadata(std::move(metadata));
}

// _____________________________________________________________________________
void DeltaTriples::consolidateAll() {
  auto consolidate = [](auto&& lt) {
    ql::ranges::for_each(lt, &LocatedTriplesPerBlock::consolidateAllBlocks);
  };
  consolidate(locatedTriples_->getLocatedTriples<false>());
  consolidate(locatedTriples_->getLocatedTriples<true>());
  consolidate(locatedTriples_->viewLocatedTriples_ | ql::views::values);
}

// _____________________________________________________________________________
template <DeltaTriples::Consolidate consolidate>
void DeltaTriples::consolidateIfRequested(
    ad_utility::timer::TimeTracer& tracer) {
  if constexpr (consolidate == Consolidate::Yes) {
    tracer.beginTrace("consolidate");
    consolidateAll();
    tracer.endTrace("consolidate");
  }
}

// _____________________________________________________________________________
void DeltaTriples::updateAugmentedMetadata() {
  auto update = [](auto&& lt) {
    ql::ranges::for_each(lt, &LocatedTriplesPerBlock::updateAugmentedMetadata);
  };
  update(locatedTriples_->getLocatedTriples<false>());
  update(locatedTriples_->getLocatedTriples<true>());
  update(locatedTriples_->viewLocatedTriples_ | ql::views::values);
}

// _____________________________________________________________________________
void DeltaTriples::writeToDisk() const {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  // TODO<RobinTF> Currently this only writes non-internal delta triples to
  // disk. The internal triples will be regenerated when importing the rest
  // again. In the future we might to also want to explicitly store the
  // internal triples.
  auto toRange = [](const TriplesSets<false>::TriplesSet& map) {
    return map |
           ql::views::transform(
               [](const IdTriple<0>& triple) -> const std::array<Id, 4>& {
                 return triple.ids();
               }) |
           ql::views::join;
  };
  ql::filesystem::path tempPath = filenameForPersisting_.value();
  tempPath += ".tmp";
  ad_utility::serializeIds(
      tempPath, localVocab_,
      std::array{toRange(triplesSetsNormal_.triplesDeleted_),
                 toRange(triplesSetsNormal_.triplesInserted_)});
  ql::filesystem::rename(tempPath, filenameForPersisting_.value());

  // NOTE: This rewrites the file of every registered view (including the whole
  // `localVocab_`) on every write, like for the triples above. Only writing the
  // changed views would be cheaper.
  for (const auto& [name, view] : views_) {
    auto filename = viewFilename(name);
    if (view.rowsInserted_.empty() && view.rowsDeleted_.empty()) {
      ql::filesystem::remove(filename);
      continue;
    }
    auto flatten = [](const ViewState::RowSet& rows) {
      return ::ranges::to_vector(rows | ql::views::join);
    };
    ql::filesystem::path tempViewPath = filename;
    tempViewPath += ".tmp";
    ad_utility::serializeIds(
        tempViewPath, localVocab_,
        std::array{flatten(view.rowsDeleted_), flatten(view.rowsInserted_),
                   view.identity_});
    ql::filesystem::rename(tempViewPath, filename);
  }
}

// _____________________________________________________________________________
void DeltaTriples::readFromDisk() {
  if (!filenameForPersisting_.has_value()) {
    return;
  }
  AD_CONTRACT_CHECK(localVocab_.empty());
  auto [vocab, idRanges] = ad_utility::deserializeIds(
      filenameForPersisting_.value(), index_.getLocalVocabContext());
  if (idRanges.empty()) {
    return;
  }
  AD_CORRECTNESS_CHECK(idRanges.size() == 2);
  auto toTriples = [](const std::vector<Id>& ids) {
    Triples triples;
    static_assert(Triples::value_type::PayloadSize == 0);
    constexpr size_t cols = Triples::value_type::NumCols;
    AD_CORRECTNESS_CHECK(ids.size() % cols == 0);
    triples.reserve(ids.size() / cols);
    for (size_t i = 0; i < ids.size(); i += cols) {
      triples.emplace_back(
          std::array{ids[i], ids[i + 1], ids[i + 2], ids[i + 3]});
    }
    // `insertTriples` and `deleteTriples` require the triples to be sorted.
    // `writeToDisk` serializes the triples in the order returned by the
    // HashMap, which is not necessarily sorted. Sort the triples when reading
    // them from disk.
    ql::ranges::sort(triples);
    return triples;
  };
  auto cancellationHandle =
      std::make_shared<CancellationHandle::element_type>();
  insertTriples<Consolidate::No>(cancellationHandle, toTriples(idRanges.at(1)));
  deleteTriples<Consolidate::No>(cancellationHandle, toTriples(idRanges.at(0)));
  consolidateAll();
  AD_LOG_INFO << "Done, #inserted triples = " << idRanges.at(1).size()
              << ", #deleted triples = " << idRanges.at(0).size() << std::endl;
}

// _____________________________________________________________________________
void DeltaTriples::setPersists(std::optional<std::string> filename) {
  filenameForPersisting_ = std::move(filename);
}

// _____________________________________________________________________________
bool DeltaTriples::persists() const {
  return filenameForPersisting_.has_value();
}

// _____________________________________________________________________________
void DeltaTriplesManager::setFilenameForPersistentUpdates(std::string filename,
                                                          bool readFromDisk) {
  modify<void>(
      [&filename, readFromDisk](DeltaTriples& deltaTriples) {
        deltaTriples.setPersists(std::move(filename));
        if (readFromDisk) {
          deltaTriples.readFromDisk();
        }
      },
      false);
}

// _____________________________________________________________________________
bool DeltaTriplesManager::persists() const {
  return deltaTriples_.rlock()->persists();
}

// _____________________________________________________________________________
std::pair<
    std::vector<LocalVocabIndex>,
    std::vector<
        ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>
DeltaTriples::copyLocalVocab() const {
  AD_CORRECTNESS_CHECK(localVocab_.otherSets().empty(),
                       "This function only copies from the primary word set.");
  std::vector<LocalVocabIndex> entries = ::ranges::to_vector(
      localVocab_.primaryWordSet() |
      ql::views::transform(
          [](const LocalVocabEntry& entry) { return &entry; }));
  return std::make_pair(std::move(entries),
                        localVocab_.getOwnedLocalBlankNodeBlocks());
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
void DeltaTriples::addFromSnapshotDiff(
    const LocatedTriplesState& oldState, const LocatedTriplesState& newState,
    const qlever::indexRebuilder::IndexRebuildMapping& idMapping,
    CancellationHandle cancellationHandle,
    ad_utility::timer::TimeTracer& tracer) {
  // NOTE: The located rows of materialized views are deliberately ignored:
  // the views are not rebuilt together with the index (their `Id`s refer to the
  // vocabulary of the old index) and are not available for the rebuilt index,
  // so their updates are dropped together with them.
  tracer.beginTrace("computeLocatedTriplesDiff");
  auto difference = computeLocatedTriplesDiff(oldState, newState);
  difference.remapIds([this, &idMapping](Id& id) {
    remapId(idMapping, id, localVocab_, index_);
  });
  tracer.endTrace("computeLocatedTriplesDiff");
  tracer.beginTrace("insertDiffedTriples");
  auto addTriples = [this, &cancellationHandle, &difference, &tracer](
                        auto isInternal, auto insertOrDelete) {
    modifyTriplesImpl<isInternal, insertOrDelete>(
        cancellationHandle,
        std::move(difference.triples<isInternal, insertOrDelete>()), tracer);
  };
  using namespace ad_utility::use_value_identity;
  addTriples(vi<false>, vi<true>);
  addTriples(vi<false>, vi<false>);
  addTriples(vi<true>, vi<true>);
  addTriples(vi<true>, vi<false>);
  tracer.endTrace("insertDiffedTriples");
  // The four calls above bypass `insertTriples`/`deleteTriples` and thus do
  // not consolidate. Consolidation is required before any read access, in
  // particular before the `updateAugmentedMetadata` that follows in
  // `DeltaTriplesManager::modify`.
  tracer.beginTrace("consolidate");
  consolidateAll();
  tracer.endTrace("consolidate");
  // Update the index of the located triples to mark that they have changed.
  locatedTriples_->index_++;
}

// _____________________________________________________________________________
void DeltaTriples::remapId(
    const qlever::indexRebuilder::IndexRebuildMapping& idMapping, Id& id,
    LocalVocab& localVocab, const IndexImpl& index) {
  const auto& [insertionPositions, localVocabMapping, blankNodeBlocks,
               minBlankNodeIndex] = idMapping;
  auto type = id.getDatatype();
  if (type == Datatype::VocabIndex) {
    id = qlever::indexRebuilder::remapVocabId(id, insertionPositions);
  } else if (type == Datatype::LocalVocabIndex) {
    auto it = localVocabMapping.find(id.getBits());
    // If we have a mapping, this means that the new index used this to make a
    // vocab index out of it and we have to do the same.
    if (it != localVocabMapping.end()) {
      id = it->second;
    } else {
      // Without a mapping the id remains of type `LocalVocabIndex` (a word
      // first seen by updates after the rebuild snapshot was taken). It still
      // points to an entry that is anchored to the OLD index; in particular,
      // its lazily cached position refers to the old vocabulary. Insert a
      // *re-anchored* copy (anchored to the new index, with an empty position
      // cache) into the local vocab and rewrite the id, so that no entry of the
      // new index references the old index, which is destroyed after the swap.
      id = Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
          LocalVocabEntry{id.getLocalVocabIndex()->asLiteralOrIri(),
                          index.getLocalVocabContext()}));
    }
  } else if (type == Datatype::BlankNodeIndex) {
    auto value = qlever::indexRebuilder::tryRemapBlankNodeId(
        id, blankNodeBlocks, minBlankNodeIndex);
    // If we have a mapping for the given blank node index, this means that the
    // block was remapped by the index rebuild. We might potentially map blank
    // node indices that were added after the mapping was created, but still
    // fall into the same allocation blocks. This is not a problem, since any
    // blank node ids that get allocated outside the interval [0,
    // minBlankNodeIndex) will get remapped on insertion. If we don't have a
    // mapping this means we don't have a mapping and keep the value as-is so it
    // gets remapped when inserted into the delta triples.
    if (value.has_value()) {
      id = value.value();
    }
  }
}
#endif

// _____________________________________________________________________________
DeltaTriples::LocatedTriplesDiff::LocatedTriplesDiff(Triples inserted,
                                                     Triples deleted,
                                                     Triples internalInserted,
                                                     Triples internalDeleted)
    : data_{std::move(inserted), std::move(deleted),
            std::move(internalInserted), std::move(internalDeleted)} {}

// ____________________________________________________________________________
template <typename Func>
void DeltaTriples::LocatedTriplesDiff::remapIds(Func func) {
  ql::ranges::for_each(data_, [&func](auto& triples) {
    ql::ranges::for_each(triples, [&func](auto& triple) {
      ql::ranges::for_each(triple.ids(), func);
    });
  });
}

// ____________________________________________________________________________
template <bool isInternal, bool insertOrDelete>
DeltaTriples::Triples& DeltaTriples::LocatedTriplesDiff::triples() {
  size_t index =
      (isInternal ? 2 : 0) + (1 - static_cast<size_t>(insertOrDelete));
  return data_.at(index);
}

// _____________________________________________________________________________
DeltaTriples::LocatedTriplesDiff DeltaTriples::computeLocatedTriplesDiff(
    const LocatedTriplesState& oldState, const LocatedTriplesState& newState) {
  auto computeDifference = [&oldState, &newState](
                               auto isInternal, Permutation::Enum permutation) {
    return newState.getLocatedTriplesForPermutation<isInternal>(permutation)
        .computeDiff(
            oldState.getLocatedTriplesForPermutation<isInternal>(permutation));
  };
  auto [insertions, deletions] =
      computeDifference(std::bool_constant<false>{}, Permutation::SPO);
  auto [internalInsertions, internalDeletions] =
      computeDifference(std::bool_constant<true>{}, Permutation::PSO);
  return LocatedTriplesDiff{std::move(insertions), std::move(deletions),
                            std::move(internalInsertions),
                            std::move(internalDeletions)};
}
