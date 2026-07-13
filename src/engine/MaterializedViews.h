// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "engine/MaterializedViewsQueryAnalysis.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/ValueId.h"
#include "index/DeltaTriples.h"
#include "index/ExternalSortFunctors.h"
#include "index/Permutation.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/QleverTypes.h"
#include "parser/GraphPatternOperation.h"
#include "parser/MaterializedViewQuery.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlTriple.h"
#include "util/HashMap.h"
#include "util/Synchronized.h"

// Forward declarations
class QueryExecutionContext;
class QueryExecutionTree;
class IndexScan;
class MaterializedViewsManager;
class IndexImpl;
class Index;

// For the future, materialized views save their version. If we change something
// about the way materialized views are stored, we can break the existing ones
// cleanly without breaking the entire index format.
static constexpr size_t MATERIALIZED_VIEWS_VERSION = 1;

// The ID of a materialized view. Not every view needs one: assigning an ID is
// only necessary for views that are referenced in a way that requires a
// compact, fixed identifier (e.g. views containing local-vocabulary entries,
// which need such an encoding, planned in a follow-up). Whether a view needs
// an ID can only be decided while its contents are being written (for the
// local-vocabulary case: whether any occur in the result at all), so the
// `MaterializedViewWriter` itself decides this, rather than the caller that
// requests the view to be written. By default a view does not get an ID. If
// needed, each view of an index gets a unique and fixed ID in the range
// `[0, MATERIALIZED_VIEW_MAX_ID]`, assigned while the view is written. At
// query time, a view that has an ID knows it from its `viewinfo.json`. The
// full list of all views that have an ID assigned is only needed when writing
// a view; it is kept in a single JSON file per index
// (`<onDiskBase>.views.json`).
using MaterializedViewId = uint64_t;

// The largest valid materialized view ID (inclusive). The limit ensures that a
// view ID always fits into 12 bits, which reserves it for compact encodings in
// the future.
static constexpr MaterializedViewId MATERIALIZED_VIEW_MAX_ID = (1ULL << 12) - 1;

// The `MaterializedViewWriter` can be used to write a new materialized view to
// disk, given an already planned query. The query will be executed lazily and
// the results will be written to the view.
class MaterializedViewWriter {
 private:
  // Filename components for writing the view to disk.
  std::string onDiskBase_;
  std::string name_;

  // The manager that owns the central views list, used to assign this view a
  // fixed ID once `computeResultAndWritePermutation` has determined (from the
  // query result) that one is needed.
  const MaterializedViewsManager* manager_;

  // The fixed ID assigned to this view, if it needs one. Unset until
  // `computeResultAndWritePermutation` has decided whether one is needed. If
  // set, it is stored in the view's metadata.
  std::optional<MaterializedViewId> viewId_;

  // Query plan to retrieve the view's rows.
  std::shared_ptr<const QueryExecutionTree> qet_;
  std::shared_ptr<const QueryExecutionContext> qec_;
  ParsedQuery parsedQuery_;

  // Memory limit and allocator for `CompressedExternalIdTableSorter`, which is
  // used if the query result is not correctly sorted.
  ad_utility::MemorySize memoryLimit_;
  ad_utility::AllocatorWithLimit<Id> allocator_;

  // The correctly ordered column names of the view.
  std::vector<Variable> columnNames_;

  // The columns of the `IdTable`s we get when executing the query can be in
  // arbitrary order. This permutation needs to be applied to get `IdTable`s
  // with the same column ordering as the `SELECT` statement.
  std::vector<ColumnIndex> columnPermutation_;

  // The number of empty columns to add to the query result such that the
  // resulting table has at least four columns.
  uint8_t numAddEmptyColumns_;

  using RangeOfIdTables = ad_utility::InputRangeTypeErased<IdTableStatic<0>>;
  // SPO comparator
  using Comparator = SortTriple<0, 1, 2>;
  // Sorter for SPO permutation with a dynamic number of columns (template
  // argument `NumStaticCols == 0`)
  using Sorter = ad_utility::CompressedExternalIdTableSorter<Comparator, 0>;

  using PlannedQuery = qlever::PlannedQuery;

  // Initialize a writer given the base filename of the view and a planned
  // query. The view will be written to files prefixed with the index basename
  // followed by the view name. `manager` is used to assign this view a fixed
  // ID, if one turns out to be needed.
  MaterializedViewWriter(std::string onDiskBase, std::string name,
                         const PlannedQuery& plannedQuery,
                         const MaterializedViewsManager* manager,
                         ad_utility::MemorySize memoryLimit,
                         ad_utility::AllocatorWithLimit<Id> allocator);

  // Get the base filename for the view's permutation and metadata files. This
  // name is the result of concatenating `onDiskBase` and `name`.
  std::string getFilenameBase() const;

  // Helper that computes the column ordering how the `IdTable`s from executing
  // the `QueryExecutionTree` must be permuted to match the requested target
  // columns and column ordering. This is called in the constructor to populate
  // `columnNamesAndPermutation_`.
  using ColumnNameAndIndex = std::pair<Variable, ColumnIndex>;
  struct ColumnNamesAndPermutation {
    std::vector<ColumnNameAndIndex> columnNamesAndIndices_;
    uint8_t numAddEmptyColumns_;
  };
  ColumnNamesAndPermutation getIdTableColumnNamesAndPermutation() const;

  // The number of columns of the view.
  size_t numCols() const {
    return columnPermutation_.size() + numAddEmptyColumns_;
  }

  // Helper to permute an `IdTable` according to `columnPermutation_` (and add
  // the empty padding columns).
  void permuteIdTable(IdTable& block) const;

  // Like `permuteIdTable`, but additionally throws if any selected column
  // contains a `LocalVocabIndex`. Used on the streaming (lazy-result) path,
  // which cannot build a per-view vocabulary; see `getBlocksWithViewVocabulary`
  // for the (fully-materialized) path that does support local-vocabulary
  // entries.
  void permuteIdTableAndCheckNoLocalVocabEntries(IdTable& block) const;

  // Handle a fully-materialized result that contains local-vocabulary entries:
  // permute and sort the single `IdTable`, then build the view's dedicated
  // vocabulary and replace the `LocalVocabIndex` values (see
  // `buildViewVocabularyAndReplace`). Returns the single sorted, permuted and
  // rewritten block.
  RangeOfIdTables getBlocksWithViewVocabulary(
      std::shared_ptr<const Result> result);

  // Given a sorted, permuted `IdTable` that may contain `LocalVocabIndex`
  // values, replace each such value by its cheapest persistent representation:
  // a `VocabIndex`/`EncodedVal` if the word is in the main index, otherwise a
  // `ViewVocabIndex` into a newly built, dedicated view vocabulary (written to
  // disk). If any `ViewVocabIndex` is needed, this assigns the view a fixed ID
  // via `manager_`. The replacement preserves the row order (so the table stays
  // sorted).
  void buildViewVocabularyAndReplace(IdTable& table);

  // Helper for `computeResultAndWritePermutation`: If the query given by the
  // user is already sorted correctly, this function can be used to obtain the
  // permuted blocks.
  RangeOfIdTables getBlocksForAlreadySortedResult(
      std::shared_ptr<const Result> result) const;

  // Helper for `computeResultAndWritePermutation`: If the query given by the
  // user is not sorted correctly, this function can be used to invoke the
  // external sorted and obtain sorted and correctly permuted blocks.
  RangeOfIdTables getBlocksForUnsortedResult(
      Sorter& spoSorter, std::shared_ptr<const Result> result) const;

  // Helper for `computeResultAndWritePermutation`: Checks if the result is
  // correctly sorted and invokes `getBlocksForAlreadySortedResult` or
  // `getBlocksForUnsortedResult` accordingly.
  RangeOfIdTables getSortedBlocks(Sorter& spoSorter,
                                  std::shared_ptr<const Result> result) const;

  // Helper for `computeResultAndWritePermutation`: given sorted and permuted
  // blocks from `getSortedBlocks`, write the `Permutation` to disk using
  // `CompressedRelationWriter`. Returns the permutation metadata.
  IndexMetaData writePermutation(RangeOfIdTables sortedBlocksSPO) const;

  // Helper for `computeResultAndWritePermutation`: Writes the metadata JSON
  // files with column names and ordering to disk.
  void writeViewMetadata() const;

  // Actually computes, permutes and if needed externally sorts the query result
  // and writes the view (SPO permutation and metadata) to disk. Once the
  // permutation has been written (and it is therefore known whether the view
  // needs a fixed ID), assigns one via `manager_` if needed and stores it in
  // `viewId_` before writing the metadata.
  void computeResultAndWritePermutation();

  friend MaterializedViewsManager;
};

// This class represents a single loaded `MaterializedView`. It can be used for
// `IndexScan`s.
class MaterializedView : public std::enable_shared_from_this<MaterializedView> {
 private:
  std::string onDiskBase_;
  std::string name_;
  std::shared_ptr<Permutation> permutation_{std::make_shared<Permutation>(
      Permutation::Enum::SPO, ad_utility::makeUnlimitedAllocator<Id>(), name_)};
  VariableToColumnMap varToColMap_;
  std::shared_ptr<LocatedTriplesState> locatedTriplesState_;
  std::optional<std::string> originalQuery_;
  std::optional<ParsedQuery> parsedQuery_;

  // The fixed ID of this view, as read from its metadata. May be unset for
  // views written before view IDs were introduced.
  std::optional<MaterializedViewId> viewId_;

  // The view's dedicated vocabulary, if it has one (i.e. its write query
  // produced local-vocabulary words that are neither in the main index nor
  // encodable). Used to resolve the index part of a `ViewVocabIndex` to its
  // string.
  std::optional<RdfsVocabulary> viewVocab_;

  // Lookup table for `BIND` statements from the view's query. Maps the cache
  // keys of the `BIND` expressions (based on the column indices in the view) to
  // the target column index.
  materializedViewsQueryAnalysis::BindExpressionAndTargetCol coveredBinds_;

  using AdditionalScanColumns = SparqlTripleSimple::AdditionalScanColumns;

  // Helper to create an empty `LocatedTriplesState` for `IndexScan`s as
  // materialized views do not support updates yet.
  std::shared_ptr<LocatedTriplesState> makeEmptyLocatedTriplesState() const;

  FRIEND_TEST(MaterializedViewsTest, ManualConfigurations);

 public:
  // Load a materialized view from disk given the filename components. The
  // constructor will throw an exception if the name is invalid or the view does
  // not exist.
  MaterializedView(std::string onDiskBase, std::string name);

  // Connect the permutation's back-reference to this view. Must be called
  // after the `MaterializedView` is managed by a `shared_ptr`.
  void connectPermutationBackReference();

  // Get the name of the view.
  const std::string& name() const { return name_; }

  // Get the variable to column map.
  const VariableToColumnMap& variableToColumnMap() const {
    return varToColMap_;
  }

  // Get the original query string used for writing the view.
  const std::optional<std::string>& originalQuery() const {
    return originalQuery_;
  }

  // Get the fixed ID of this view, if it has one.
  const std::optional<MaterializedViewId>& id() const { return viewId_; }

  // Whether this view has a dedicated vocabulary (see `viewVocab_`).
  bool hasVocabulary() const { return viewVocab_.has_value(); }

  // Resolve the index part of a `ViewVocabIndex` (the `indexInVocab`) to its
  // string via this view's dedicated vocabulary. Must only be called if
  // `hasVocabulary()`.
  std::string resolveViewVocabString(uint64_t indexInVocab) const;

  // Filename suffix (appended to the view's base filename, see
  // `getFilenameBase`) and vocabulary type of a view's dedicated vocabulary.
  // Shared by the writer and the reader.
  static constexpr std::string_view viewVocabularySuffix_ = ".vocabulary";
  static ad_utility::VocabularyType viewVocabularyType();

  // Get a parsed version of the original query, used for query analysis.
  const std::optional<ParsedQuery>& parsedQuery() const { return parsedQuery_; }

  // Return the combined filename from the index' `onDiskBase` and the name of
  // the view. Note that this function does not check for validity or existence.
  static std::string getFilenameBase(std::string_view onDiskBase,
                                     std::string_view name);

  // Return a pointer to the open `Permutation` object for this view. Note that
  // this is always an SPO permutation because materialized views are indexed on
  // the first column. The result of this function is guaranteed to never be
  // `nullptr`.
  std::shared_ptr<const Permutation> permutation() const;

  // Return a reference to the `LocatedTriplesSnapshot` for the permutation. For
  // now this is always an empty snapshot but with the correct permutation
  // metadata.
  LocatedTriplesSharedState locatedTriplesState() const;

  // Checks if the given name is allowed for a materialized view. Currently only
  // alphanumerics and hyphens are allowed. This is relevant for safe filenames
  // and for correctly splitting the special predicate.
  static bool isValidName(std::string_view name);
  static void throwIfInvalidName(std::string_view name);

  // Given a `MaterializedViewQuery` obtained from a special `SERVICE` or
  // predicate, compute the `SparqlTripleSimple` to be passed to the constructor
  // of `IndexScan` such that the columns requested by the user are returned.
  SparqlTripleSimple makeScanConfig(
      const parsedQuery::MaterializedViewQuery& viewQuery) const;

  // Helpers for checking metadata-dependent invariants of
  // `MaterializedViewQuery` in `makeScanConfig`.
  void throwIfScanColumnMissing(const std::optional<TripleComponent>& s) const;
  void throwIfColumnsHaveIllegalFixedValues(
      const std::optional<TripleComponent>& s, const TripleComponent& p,
      const TripleComponent& o) const;
  void throwIfColumnNotInView(const Variable& column) const;
  void throwIfAdditionalColumnIsNotVariable(const Variable& column,
                                            const TripleComponent& value) const;
  void throwIfScanColumnIsSetTwice(const std::optional<TripleComponent>& s,
                                   const TripleComponent& value) const;
  void throwIfVariableUsedTwice(
      const ad_utility::HashSet<Variable>& variablesSeen,
      const TripleComponent& target) const;

  // Given a `QueryExecutionContext` and the arguments for `makeScanConfig`
  // construct an `IndexScan` operation for scanning the requested columns
  // of this view. The result of this function is guaranteed to never be
  // `nullptr`.
  std::shared_ptr<IndexScan> makeIndexScan(
      QueryExecutionContext* qec,
      const parsedQuery::MaterializedViewQuery& viewQuery) const;

  // If the materialized view contains a top-level `BIND` statement where the
  // expression matches the given cache key, return the column index of the
  // `BIND`'s target variable.
  //
  // IMPORTANT: This works only if the caller can guarantee that the variables
  // in the `BIND` expression have been mapped to exactly the same column
  // indices as in the view while generating the cache key.
  std::optional<size_t> lookupBindTargetColumn(
      const std::string& bindCacheKey) const;

  // Dummy variables for internal use.
  static const Variable& dummyPredicate();
  static const Variable& dummyObject();
};

// Shorthand for query rewriting helper class.
using materializedViewsQueryAnalysis::MaterializedViewJoinReplacement;

// The `MaterializedViewsManager` is part of the `QueryExecutionContext` and is
// used to manage the currently loaded `MaterializedViews` in a `Server` or
// `Qlever` instance.
class MaterializedViewsManager : public ViewVocabComparisonHooks {
 private:
  std::string onDiskBase_;

  // Pointer to the main index. Needed by the `ViewVocabComparisonHooks`
  // implementation (see below) to compute main-vocabulary buckets and to
  // resolve/compare against main-vocabulary and encoded IRIs. Set via
  // `setIndex`, and stable because the manager and the index live together in
  // a non-moveable `Qlever::IndexAndViews`.
  const IndexImpl* index_ = nullptr;

  // Helper struct to unify the locking of loaded views and `QueryPatternCache`.
  struct LoadedViews {
    ad_utility::HashMap<std::string, std::shared_ptr<MaterializedView>> views_;
    materializedViewsQueryAnalysis::QueryPatternCache queryPatternCache_;
    // Views that have a fixed ID, indexed by that ID, so that a
    // `ViewVocabIndex` (which stores the view ID) can be resolved.
    ad_utility::HashMap<MaterializedViewId, std::shared_ptr<MaterializedView>>
        viewsById_;
  };

  mutable ad_utility::Synchronized<LoadedViews> loadedViews_;

  // Guards the read-modify-write of the central views list file
  // (`<onDiskBase>.views.json`) when writing views. Stored via `unique_ptr` so
  // that `MaterializedViewsManager` remains moveable (required by
  // `IndexAndViews`).
  mutable std::unique_ptr<std::mutex> viewsListMutex_{
      std::make_unique<std::mutex>()};

  // The central list of the materialized views of the index that have a fixed
  // ID, mapping each such view's name to its ID. Views that do not need an ID
  // are not listed here. This list is only needed when writing views; at
  // query time, each view knows its own ID (if any) from its `viewinfo.json`.
  using ViewsList = ad_utility::HashMap<std::string, MaterializedViewId>;

  // Return the filename of the central views list file for this index.
  std::string viewsListFilename() const;

  // Read the central views list from disk (empty if the file does not exist).
  ViewsList readViewsList() const;

  // Persist the central views list to disk as a JSON object mapping names to
  // IDs.
  void writeViewsList(const ViewsList& views) const;

  // Return the smallest free ID in `[0, MATERIALIZED_VIEW_MAX_ID]` given the
  // currently used IDs, throwing if no free ID is available.
  static MaterializedViewId smallestFreeViewId(const ViewsList& views);

  // Assign the view with the given name a fixed ID, recording it in the
  // central views list, and return it. Called by a `MaterializedViewWriter`
  // once it has determined (from the query result) that its view needs one.
  MaterializedViewId assignId(const std::string& name) const;

  friend class MaterializedViewWriter;
  FRIEND_TEST(MaterializedViewsTest, ViewIdsAndCentralList);
  FRIEND_TEST(MaterializedViewsTest, ViewIdsCorruptedFiles);

 public:
  MaterializedViewsManager() = default;
  explicit MaterializedViewsManager(std::string onDiskBase)
      : onDiskBase_{std::move(onDiskBase)} {};

  // Unregister this manager as the process-wide `ViewVocabComparisonHooks` if
  // it was registered (see `loadView`).
  ~MaterializedViewsManager() override;

  // Keep the manager moveable (required by `Qlever::IndexAndViews`); the
  // user-declared destructor would otherwise suppress the moves. Moves only
  // happen at construction time, before any view (and thus any hook
  // registration) is loaded, so the registered `this` pointer never becomes
  // stale through a move.
  MaterializedViewsManager(MaterializedViewsManager&&) noexcept = default;
  MaterializedViewsManager& operator=(MaterializedViewsManager&&) noexcept =
      default;

  // For use with the default constructor: set the index basename after creation
  // of the `MaterializedViewsManager`. This should only be called once and
  // before any calls to `loadView` and `getView`.
  void setOnDiskBase(const std::string& onDiskBase);

  // Set the main index used by the `ViewVocabComparisonHooks` implementation.
  // Must be called once (before any view with a vocabulary is loaded), with an
  // index that outlives this manager (they live together in
  // `Qlever::IndexAndViews`).
  void setIndex(const Index& index);

  // `ViewVocabComparisonHooks` implementation (used by `ValueId` comparison and
  // hashing for `ViewVocabIndex` values). See the base class for the contract.
  std::string viewVocabString(ValueId viewVocabIndexId) const override;
  ql::strong_ordering compareByString(ValueId a, ValueId b) const override;
  uint64_t mainVocabBucket(ValueId mainVocabId) const override;

  // Check if a materialized view is currently loaded.
  bool isViewLoaded(const std::string& name) const;

  // Since we don't want to break the const-ness in a lot of places just for the
  // loading of views, `loadedViews_` is mutable. Note that this is okay,
  // because the views themselves aren't changed (only loaded on-demand).
  void loadView(const std::string& name) const;

  // Unload a materialized view if it is loaded. This function is a no-op
  // otherwise. It is `const` for the same reason described above.
  void unloadViewIfLoaded(const std::string& name) const;

  // Load the given view if it is not already loaded and return it. This pointer
  // is never `nullptr`. If the view does not exist, the function throws.
  std::shared_ptr<const MaterializedView> getView(
      const std::string& name) const;

  // The same as `MaterializedView::makeIndexScan` above, but load and use the
  // right view automatically as requested in the `MaterializedViewQuery`.
  std::shared_ptr<IndexScan> makeIndexScan(
      QueryExecutionContext* qec,
      const parsedQuery::MaterializedViewQuery& viewQuery) const;

  // Given a set of triples, check if some join operations that would be
  // required when evaluating them can be replaced by scans on materialized
  // views that are currently loaded. This is implemented using the
  // `queryPatternCache_`.
  std::vector<MaterializedViewJoinReplacement> makeJoinReplacementIndexScans(
      QueryExecutionContext* qec,
      const parsedQuery::BasicGraphPattern& triples) const;

  // Write a `MaterializedView` given a valid `name` (consisting only of
  // alphanumerics and hyphens) and a `plannedQuery` to be executed. The query's
  // result is written to the view. Whether the view is assigned a fixed
  // `MaterializedViewId` is decided internally by the `MaterializedViewWriter`
  // itself while writing (see the comment on `MaterializedViewId`); most views
  // do not need one.
  //
  // If a view with the same name is already loaded, it is unloaded before
  // writing.
  //
  // The `memoryLimit` and `allocator` are used only for sorting the
  // permutation if the query result is not correctly sorted already. The
  // `plannedQuery` is executed with the normal query memory limit.
  void writeViewToDisk(
      std::string name, const qlever::PlannedQuery& plannedQuery,
      ad_utility::MemorySize memoryLimit = ad_utility::MemorySize::gigabytes(4),
      ad_utility::AllocatorWithLimit<Id> allocator =
          ad_utility::makeUnlimitedAllocator<Id>()) const;
};

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
