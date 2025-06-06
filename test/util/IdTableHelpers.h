// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_TEST_UTIL_IDTABLEHELPERS_H
#define QLEVER_TEST_UTIL_IDTABLEHELPERS_H

#include <algorithm>
#include <concepts>
#include <cstdio>
#include <fstream>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <tuple>

#include "../engine/ValuesForTesting.h"
#include "./AllocatorTestHelpers.h"
#include "./GTestHelpers.h"
#include "./IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "util/Algorithm.h"
#include "util/Forward.h"
#include "util/Random.h"

/*
 * Does what it says on the tin: Save an IdTable with the corresponding
 * join column.
 */
struct IdTableAndJoinColumn {
  IdTable idTable;
  size_t joinColumn;
};

// Implementation of a class that inherits from `IdTable` but is copyable
// (convenient for testing).
template <size_t N = 0>
using TableImpl = std::conditional_t<N == 0, IdTable, IdTableStatic<N>>;
template <size_t N = 0>
class CopyableIdTable : public TableImpl<N> {
 public:
  using Base = TableImpl<N>;
  using Base::Base;
  CopyableIdTable(const CopyableIdTable& rhs) : Base{rhs.clone()} {}
  CopyableIdTable& operator=(const CopyableIdTable& rhs) {
    static_cast<Base&>(*this) = rhs.clone();
    return *this;
  }
};

// For easier reading. We repeat that type combination so often, that this
// will make things a lot easier in terms of reading and writing.
using IntOrId = std::variant<int64_t, Id>;
using VectorTable = std::vector<std::vector<IntOrId>>;

/*
 * Return an 'IdTable' with the given `content` by applying the
 * `transformation` to each of them. All rows of `content` must have the
 * same length.
 */
template <typename Transformation = decltype(ad_utility::testing::VocabId)>
IdTable makeIdTableFromVector(const VectorTable& content,
                              Transformation transformation = {}) {
  size_t numCols = content.empty() ? 0UL : content.at(0).size();
  IdTable result{numCols, ad_utility::testing::makeAllocator()};
  result.reserve(content.size());
  for (const auto& row : content) {
    AD_CONTRACT_CHECK(row.size() == result.numColumns());
    result.emplace_back();
    for (size_t i = 0; i < result.numColumns(); ++i) {
      if (std::holds_alternative<Id>(row.at(i))) {
        result.back()[i] = std::get<Id>(row.at(i));
      } else {
        result.back()[i] = transformation(std::get<int64_t>(row.at(i)));
      }
    }
  }
  return result;
}

// Similar to `makeIdTableFromVector` (see above), but returns a GMock
// `matcher`, that matches for equality with the created `IdTable`. In
// particular, the matcher also deals with `IdTable` not being copyable, which
// requires a workaround for GMock/GTest.
struct MatchesIdTableFromVector {
  template <typename Transformation = decltype(ad_utility::testing::VocabId)>
  auto operator()(const VectorTable& content, Transformation t = {}) const {
    return ::testing::Eq(
        CopyShield<IdTable>(makeIdTableFromVector(content, std::move(t))));
  }
};
static constexpr MatchesIdTableFromVector matchesIdTableFromVector;

// Construct an `IdTable` from the given arguments, but returns a GMock
// `matcher`, that matches for equality with the `IdTable`. In particular, the
// matcher also deals with `IdTable` not being copyable, which requires a
// workaround for GMock/GTest.
struct MatchesIdTable {
  template <typename... Ts>
  requires(std::constructible_from<IdTable, Ts && ...>)
  auto operator()(Ts&&... ts) const {
    return ::testing::Eq(CopyShield<IdTable>(IdTable{AD_FWD(ts)...}));
  }

  // Overload for lvalue-references (`IdTable`s are not copyable)
  template <typename T,
            typename = std::enable_if_t<ad_utility::SimilarTo<T, IdTable>>>
  auto operator()(T& table) const {
    // Note: We could use `Eq(cref(table))` , but the explicit deep copy
    // gets rid of all possibly lifetime and mutability issues.
    return operator()(table.clone());
  }
};
static constexpr MatchesIdTable matchesIdTable;

/*
 * @brief Tests, whether the given IdTable has the same content as the sample
 * solution and, if the option was chosen, if the IdTable is sorted by
 * the join column.
 *
 * @param table The IdTable that should be tested.
 * @param expectedContent The sample solution. Doesn't need to be sorted,
 *  or the same order of rows as the table.
 * @param resultMustBeSortedByJoinColumn If this is true, it will also be
 * tested, if the table is sorted by the join column.
 * @param joinColumn The join column of the table.
 * @param l Ignore it. It's only here for being able to make better messages,
 *  if a IdTable fails the comparison.
 */
void compareIdTableWithExpectedContent(
    const IdTable& table, const IdTable& expectedContent,
    const bool resultMustBeSortedByJoinColumn = false,
    const size_t joinColumn = 0,
    ad_utility::source_location l = ad_utility::source_location::current());

/*
 * @brief Sorts an IdTable in place, in the same way, that we sort them during
 * normal program usage.
 */
void sortIdTableByJoinColumnInPlace(IdTableAndJoinColumn& table);

/*
@brief Creates a `IdTable`, where the rows are created via generator.

@param numberRows numberColumns The number of rows and columns, the table should
have.
@param rowGenerator Creates the rows for the to be returned `IdTable`. The
generated row must ALWAYS have size `numberColumns`. Otherwise an exception will
be thrown.
*/
IdTable generateIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::function<std::vector<ValueId>()>& rowGenerator);

/*
@brief Create an `IdTable`, where the content of the join columns are given via
repeatedly called generator functions (one function per join column) and other
entries are random.

@param numberRows numberColumns The number of rows and columns, the table should
have.
@param joinColumnWithGenerator Every pair describes the position of a join
column and the function, which will be called, to generate it's entries.
@param randomSeed The seed for the random number generator, that generates the
content for the non join column entries.
*/
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<std::pair<size_t, std::function<ValueId()>>>&
        joinColumnWithGenerator,
    const ad_utility::RandomSeed randomSeed = ad_utility::RandomSeed::make(
        ad_utility::FastRandomIntGenerator<unsigned int>{}()));

/*
@brief Creates a `IdTable`, where the content of the join columns is given via
a function and all other columns are randomly filled with numbers.

@param numberRows numberColumns The number of rows and columns, the table should
have.
@param joinColumns The join columns.
@param generator The generator for the join columns. Order of calls: Row per
row, starting from row 0, and in a row for every join column, with the join
columns ordered by their column. Starting from column 0.
@param randomSeed The seed for the random number generator, that generates the
content for the non join column entries.
*/
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<size_t>& joinColumns,
    const std::function<ValueId()>& generator,
    const ad_utility::RandomSeed randomSeed = ad_utility::RandomSeed::make(
        ad_utility::FastRandomIntGenerator<unsigned int>{}()));

// Describes a join column together with an inclusive range of numbers, defined
// as [lowerBound, upperBound], and the seed for the random number generator.
struct JoinColumnAndBounds {
  const size_t joinColumn_;
  const size_t lowerBound_;
  const size_t upperBound_;
  const ad_utility::RandomSeed randomSeed_ = ad_utility::RandomSeed::make(
      ad_utility::FastRandomIntGenerator<unsigned int>{}());
};

/*
@brief Return a IdTable, that is randomly filled. The range of numbers
being entered in the join column can be defined.

@param numberRows, numberColumns The size of the IdTable, that is to be
returned.
@param joinColumnAndBounds The given join column will be filled with random
number, that are all inside the given range.
@param randomSeed The seed for the random number generator, that
generates the content for the non join column entries.
*/
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const JoinColumnAndBounds& joinColumnAndBounds,
    const ad_utility::RandomSeed randomSeed = ad_utility::RandomSeed::make(
        ad_utility::FastRandomIntGenerator<unsigned int>{}()));

/*
@brief Return a IdTable, that is randomly filled. The range of numbers
being entered in the join columns can be defined.

@param numberRows, numberColumns The size of the IdTable, that is to be
returned.
@param joinColumnsAndBounds Every join columns will be filled with random
number, that are inside their corresponding range.
@param randomSeed The seed for the random number generator, that
generates the content for the non join column entries.
*/
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const std::vector<JoinColumnAndBounds>& joinColumnsAndBounds,
    const ad_utility::RandomSeed randomSeed = ad_utility::RandomSeed::make(
        ad_utility::FastRandomIntGenerator<unsigned int>{}()));

/*
@brief Return a IdTable, that is completely randomly filled.

@param numberRows, numberColumns The size of the IdTable, that is to be
returned.
@param randomSeed The seed for the random number generator, that
generates the content.
*/
IdTable createRandomlyFilledIdTable(
    const size_t numberRows, const size_t numberColumns,
    const ad_utility::RandomSeed randomSeed = ad_utility::RandomSeed::make(
        ad_utility::FastRandomIntGenerator<unsigned int>{}()));

/// Turn a given `IdTable` into a `QueryExecutionTree` by cloning the table
/// and filling it with dummy variables.
std::shared_ptr<QueryExecutionTree> idTableToExecutionTree(
    QueryExecutionContext*, const IdTable&);

// Fully consume a given generator and store it in an `IdTable` and store the
// local vocabs in a vector.
std::pair<IdTable, std::vector<LocalVocab>> aggregateTables(
    Result::LazyResult generator, size_t numColumns);

// Create an `IdTable` of the given size with width 1, filled with the given
// value.
IdTable createIdTableOfSizeWithValue(size_t size, Id value);

#endif  // QLEVER_TEST_UTIL_IDTABLEHELPERS_H
