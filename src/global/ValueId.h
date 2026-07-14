//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_GLOBAL_VALUEID_H
#define QLEVER_SRC_GLOBAL_VALUEID_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <string>

#include "backports/functional.h"
#include "backports/keywords.h"
#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "global/IndexTypes.h"
#include "rdfTypes/GeoPoint.h"
#include "util/Algorithm.h"
#include "util/BitUtils.h"
#include "util/DateYearDuration.h"
#include "util/NBitInteger.h"
#include "util/Serializer/Serializer.h"
#include "util/SourceLocation.h"

// The different Datatypes that a `ValueId` (see below) can encode.
// Note: If you add a datatype, make sure to update the `MaxValue` if necessary,
// and check whether you have to add it to the `isDatatypeTrivial` function
// directly below.
enum struct Datatype {
  Undefined = 0,
  Bool,
  Int,
  Double,
  VocabIndex,
  LocalVocabIndex,
  TextRecordIndex,
  Date,
  GeoPoint,
  WordVocabIndex,
  BlankNodeIndex,
  EncodedVal,
  // Refers to a string in the dedicated vocabulary of a materialized view. The
  // 60 data bits are split into [viewId (12) | sortHelper (10) | index (38)];
  // see the `ValueId` class comment for details.
  ViewVocabIndex,
  MaxValue = ViewVocabIndex
  // Note: Unfortunately, we cannot easily get the size of an enum.
  // If members are added to this enum, then the `MaxValue`
  // alias must always be equal to the last member,
  // else other code breaks with out-of-bounds accesses.
};

// Return true iff the `datatype` is a trivial datatype. This means that IDs
// with this datatype directly encode the value they represent and do not point
// to an external resource. In other words: These IDs can safely be shared
// across different QLever indices without having to rewrite them. Note:
// `BlankNodeIndex` is deliberately NOT considiered trivial, as blank nodes
// depend on the context, in particular they have to be remapped when results
// from different  RDF sources are merged. Same goes for `EncodedVal` which
// depends on the (configurable!) prefixes for the encoding.
constexpr bool isDatatypeTrivial(Datatype datatype) {
  using enum Datatype;
  constexpr std::array trivialDatatypes{Undefined, Bool, Int,
                                        Double,    Date, GeoPoint};
  return ad_utility::contains(trivialDatatypes, datatype);
}

/// Convert the `Datatype` enum to the corresponding string
inline QL_CONSTEXPR std::string_view toString(Datatype type) {
  switch (type) {
    case Datatype::Undefined:
      return "Undefined";
    case Datatype::Bool:
      return "Bool";
    case Datatype::Double:
      return "Double";
    case Datatype::Int:
      return "Int";
    case Datatype::EncodedVal:
      return "EncodedIri";
    case Datatype::VocabIndex:
      return "VocabIndex";
    case Datatype::LocalVocabIndex:
      return "LocalVocabIndex";
    case Datatype::TextRecordIndex:
      return "TextRecordIndex";
    case Datatype::WordVocabIndex:
      return "WordVocabIndex";
    case Datatype::Date:
      return "Date";
    case Datatype::GeoPoint:
      return "GeoPoint";
    case Datatype::BlankNodeIndex:
      return "BlankNodeIndex";
    case Datatype::ViewVocabIndex:
      return "ViewVocabIndex";
  }
  // This line is reachable if we cast an arbitrary invalid int to this enum
  AD_FAIL();
}

class ValueId;

// Interface for resolving `ViewVocabIndex` `ValueId`s to their string values.
// The `MaterializedViewsManager` implements this (using the loaded views'
// vocabularies and the main index) and registers a single instance
// process-wide via `setViewVocabComparisonHooks` when it loads a view that has
// a vocabulary. `ValueId::compareThreeWay` and `AbslHashValue` use it for the
// cases that cannot be decided from the bits alone.
//
// NOTE: Because a `ViewVocabIndex` only stores a 12-bit view ID, the hooks are
// meaningful for a single index (the common deployment). This mirrors the
// `EncodedIriManager`, which is likewise per-index.
class ViewVocabComparisonHooks {
 public:
  virtual ~ViewVocabComparisonHooks() = default;

  // The string value of the word referenced by a `ViewVocabIndex` id.
  virtual std::string viewVocabString(ValueId viewVocabIndexId) const = 0;

  // Three-way comparison of the string values of `a` and `b`, using the index'
  // total (locale-aware) comparator. At least one of them is a
  // `ViewVocabIndex`; the other is a `ViewVocabIndex`, a `VocabIndex`, or a
  // `LocalVocabIndex` (an `EncodedVal` never reaches here, as it is ordered
  // against a `ViewVocabIndex` without a string comparison). This cannot be
  // reduced to a plain `std::string` comparison, which is why it is a hook and
  // not done in `ValueId`.
  virtual ql::strong_ordering compareByString(ValueId a, ValueId b) const = 0;

  // The sort-helper bucket (in `[0, 2 ** numViewSortHelperBits)`) that a
  // `VocabIndex` `id` falls into, using the same bucketing as the sort helper
  // stored in a `ViewVocabIndex`.
  virtual uint64_t mainVocabBucket(ValueId mainVocabId) const = 0;
};

namespace detail {
// Storage for the single process-wide `ViewVocabComparisonHooks`.
inline std::atomic<const ViewVocabComparisonHooks*>& viewVocabHooksStorage() {
  static std::atomic<const ViewVocabComparisonHooks*> hooks{nullptr};
  return hooks;
}
}  // namespace detail

// Register (or clear, with `nullptr`) the process-wide comparison hooks for
// `ViewVocabIndex` `ValueId`s.
inline void setViewVocabComparisonHooks(const ViewVocabComparisonHooks* hooks) {
  detail::viewVocabHooksStorage().store(hooks, std::memory_order_release);
}

// Return the registered comparison hooks, or `nullptr` if none is set.
inline const ViewVocabComparisonHooks* getViewVocabComparisonHooks() {
  return detail::viewVocabHooksStorage().load(std::memory_order_acquire);
}

// Atomically clear the hooks, but only if they currently equal `expected`.
// Used when a hooks instance is destroyed, so that it does not clobber a
// different instance that was registered concurrently in the meantime.
inline void clearViewVocabComparisonHooksIfCurrent(
    const ViewVocabComparisonHooks* expected) {
  detail::viewVocabHooksStorage().compare_exchange_strong(
      expected, nullptr, std::memory_order_acq_rel);
}

/// Encode values of different types (the types from the `Datatype` enum above)
/// using 4 bits for the datatype and 60 bits for the value.
///
/// For `ViewVocabIndex`, the 60 data bits are split into three parts (from most
/// to least significant): the materialized view ID (12 bits), a sorting helper
/// (10 bits), and the index into that view's dedicated vocabulary (38 bits).
/// The sorting helper records in which of 1024 equal-sized parts of the main
/// vocabulary the referenced word would be located; it lets most comparisons
/// against other `ValueId`s be decided without a (potentially expensive) string
/// comparison (see `compareViewVocabInvolved`). The fallback string comparison
/// and the resolution of a `ViewVocabIndex` to its string go through the
/// process-wide `ViewVocabComparisonHooks` (see above).
class ValueId {
 public:
  using T = uint64_t;
  static constexpr T numDatatypeBits = 4;
  static constexpr T numDataBits = 64 - numDatatypeBits;

  using IntegerType = ad_utility::NBitInteger<numDataBits>;

  /// The maximum value for the unsigned types that are used as indices
  /// (currently VocabIndex, LocalVocabIndex and Text).
  static constexpr T maxIndex = (1ull << numDataBits) - 1;

  /// The smallest double > 0 that will not be rounded to zero by the precision
  /// loss of `FoldedId`. Symmetrically, `-minPositiveDouble` is the largest
  /// double <0 that will not be rounded to zero.
  /// Note: This constant is currently only used in unit tests, and cannot be
  /// computed at compile time in C++17.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static constexpr double minPositiveDouble =
      absl::bit_cast<double>(1ull << numDatatypeBits);
#endif

  // The largest representable integer value.
  static constexpr int64_t maxInt = IntegerType::max();
  // All types that store strings. Together, the IDs of all the items of these
  // types form a consecutive range of IDs when sorted. Within this range, the
  // IDs are ordered by their string values, not by their IDs (and hence also
  // not by their types).
  static constexpr std::array<Datatype, 2> stringTypes_{
      Datatype::VocabIndex, Datatype::LocalVocabIndex};

  // A mapping that decides if a Datatype is bitwise comparable or not. See
  // `canBeComparedBitwise()` below. The two non-bitwise entries are
  // `LocalVocabIndex` (index 5) and `ViewVocabIndex` (index 12), both of which
  // require a string comparison to be ordered correctly against other types.
  static constexpr std::array<bool, 13> isTypeBitwiseComparable_{
      true, true, true, true, true, false, true,
      true, true, true, true, true, false};

  // Assert that the types in `stringTypes_` are directly adjacent. This is
  // required to make the comparison of IDs in `ValueIdComparators.h` work.
  static constexpr Datatype maxStringType_ = ql::ranges::max(stringTypes_);
  static constexpr Datatype minStringType_ = ql::ranges::min(stringTypes_);
  static_assert(static_cast<size_t>(maxStringType_) -
                    static_cast<size_t>(minStringType_) + 1 ==
                stringTypes_.size());

  // Assert that the size of an encoded GeoPoint equals the available bits in a
  // ValueId.
  static_assert(numDataBits == GeoPoint::numDataBits);

  /// This exception is thrown if we try to store a value of an index type
  /// (VocabIndex, LocalVocabIndex, TextRecordIndex) that is larger than
  /// `maxIndex`.
  struct IndexTooLargeException : public std::exception {
   private:
    std::string errorMessage_;

   public:
    explicit IndexTooLargeException(
        T tooBigValue,
        ad_utility::source_location s = AD_CURRENT_SOURCE_LOC()) {
      errorMessage_ = absl::StrCat(
          s.file_name(), ", line ", s.line(), ": The given value ", tooBigValue,
          " is bigger than what the maxIndex of ValueId allows.");
    }

    const char* what() const noexcept override { return errorMessage_.c_str(); }
  };

  /// A struct that represents the single undefined value. This is required for
  /// generic code like in the `visit` method.
  struct UndefinedType {};

 private:
  // The actual bits.
  T _bits;

 public:
  /// Default construction of an uninitialized id.
  ValueId() = default;

  /// Comparison is performed directly on the underlying representation. Note
  /// that because the type bits are the most significant bits, all values of
  /// the same `Datatype` will be adjacent to each other. Unsigned index types
  /// are also ordered correctly. Signed integers are ordered as follows: first
  /// the positive integers in order and then the negative integers in order.
  /// For doubles it is first the positive doubles in order, then the negative
  /// doubles in reversed order. This is a direct consequence of comparing the
  /// bit representation of these values as unsigned integers.
  constexpr auto compareThreeWay(const ValueId& other) const {
    using enum Datatype;
    auto type = getDatatype();
    auto otherType = other.getDatatype();
    // `ViewVocabIndex` needs its own (partially string-based) comparison, see
    // `compareViewVocabInvolved`.
    if (type == ViewVocabIndex || otherType == ViewVocabIndex) [[unlikely]] {
      return compareViewVocabInvolved(*this, other);
    }
    if (type != LocalVocabIndex && otherType != LocalVocabIndex) {
      return ql::compareThreeWay(_bits, other._bits);
    }
    if (type == LocalVocabIndex && otherType == LocalVocabIndex) [[unlikely]] {
      return ql::compareThreeWay(*getLocalVocabIndex(),
                                 *other.getLocalVocabIndex());
    }

    // GCC 11 issues a false positive warning here, so we try to avoid it by
    // being over-explicit about the branches here.
    if ((type == VocabIndex || type == EncodedVal) &&
        otherType == LocalVocabIndex) {
      return compareVocabAndLocalVocab(
          LocalVocabEntry::IdProxy::make(getBits()),
          other.getLocalVocabIndex());
    } else if (type == LocalVocabIndex &&
               (otherType == VocabIndex || otherType == EncodedVal)) {
      auto inverseOrder = compareVocabAndLocalVocab(
          LocalVocabEntry::IdProxy::make(other.getBits()),
          getLocalVocabIndex());

      return ql::compareThreeWay(0, inverseOrder);
    }

    // One of the types is `LocalVocab`, and the other one is a non-string
    // type like `Integer` or `Undefined. Then the comparison by bits
    // automatically compares by the datatype.
    return ql::compareThreeWay(_bits, other._bits);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(ValueId)

  friend constexpr bool operator==(const ValueId& left, const ValueId& right) {
    return ql::compareThreeWay(left, right) == 0;
  }
  friend constexpr bool operator!=(const ValueId& left, const ValueId& right) {
    return !(left == right);
  }

  // When there are no local vocab entries, then comparison can only be done
  // on the underlying bits, which allows much better code generation (e.g.
  // vectorization). In particular, this method should for example be used
  // during index building.
  auto compareWithoutLocalVocab(const ValueId& other) const {
    // NOTE: If this static assertion is violated at some point, make sure to
    // check all callers of this function if they are still correct.
    static_assert(onlyLocalAndViewVocabNotBitwiseComparable);
    AD_EXPENSIVE_CHECK(canBeComparedBitwise());
    AD_EXPENSIVE_CHECK(other.canBeComparedBitwise());
    return ql::compareThreeWay(_bits, other._bits);
  }

  /// Get the underlying bit representation, e.g. for compression etc.
  [[nodiscard]] constexpr T getBits() const noexcept { return _bits; }
  /// Construct from the underlying bit representation. `bits` must have been
  /// obtained by a call to `getBits()` on a valid `ValueId`.
  static constexpr ValueId fromBits(T bits) noexcept { return {bits}; }

  /// Get the datatype.
  [[nodiscard]] constexpr Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(_bits >> numDataBits);
  }

  /// Create a `ValueId` of the `Undefined` type. There is only one such ID and
  /// it is guaranteed to be smaller than all IDs of other types. This helps
  /// implementing the correct join behavior in presence of undefined values.
  constexpr static ValueId makeUndefined() noexcept { return {0}; }

  /// Returns an object of `UndefinedType`. In many scenarios this function is
  /// unnecessary because `getDatatype() == Undefined` already identifies the
  /// single undefined value correctly, but it is very useful for generic code
  /// like the `visit` member function.
  [[nodiscard]] UndefinedType getUndefined() const noexcept { return {}; }
  bool isUndefined() const noexcept { return *this == makeUndefined(); }

  /// Create a `ValueId` for a double value. The conversion will reduce the
  /// precision of the mantissa of an IEEE double precision floating point
  /// number from 53 to 49 significant bits.
  static ValueId makeFromDouble(double d) {
    auto shifted = absl::bit_cast<T>(d) >> numDatatypeBits;
    return addDatatypeBits(shifted, Datatype::Double);
  }
  /// Obtain the `double` that this `ValueId` encodes. If `getDatatype() !=
  /// Double` then the result is unspecified.
  [[nodiscard]] double getDouble() const noexcept {
    return absl::bit_cast<double>(_bits << numDatatypeBits);
  }

  /// Create a `ValueId` for a signed integer value. Integers in the range
  /// [-2^59, 2^59-1] can be represented. Integers outside of this range will
  /// overflow according to the semantics of `NBitInteger<60>`.
  static ValueId makeFromInt(int64_t i) noexcept {
    auto nbit = IntegerType::toNBit(i);
    return addDatatypeBits(nbit, Datatype::Int);
  }

  /// Obtain the signed integer that this `ValueId` encodes. If `getDatatype()
  /// != Int` then the result is unspecified.
  [[nodiscard]] int64_t getInt() const noexcept {
    return IntegerType::fromNBit(_bits);
  }

  /// Create a `ValueId` for a boolean value.
  static constexpr ValueId makeFromBool(bool b) noexcept {
    auto bits = static_cast<T>(b);
    return addDatatypeBits(bits, Datatype::Bool);
  }

  /// Create a `ValueId` for a boolean value, represented as "0" or "1" instead
  /// of "false" or "true".
  static constexpr ValueId makeBoolFromZeroOrOne(bool b) noexcept {
    auto bits = static_cast<T>(b);
    bits |= static_cast<T>(true) << 1;
    return addDatatypeBits(bits, Datatype::Bool);
  }

  // Obtain the boolean value.
  [[nodiscard]] bool getBool() const noexcept {
    return static_cast<bool>(removeDatatypeBits(_bits) & 1);
  }

  // Obtain the boolean value as a string view. In particular, return either
  // `true`, `false`, `0` , or `1`, depending on whether the value was created
  // via `makeFromBool` or `makeBoolFromZeroOrOne` (see above).
  std::string_view getBoolLiteral() const noexcept {
    bool value = getBool();
    if (_bits & 0b10) {
      return value ? "1" : "0";
    }
    return value ? "true" : "false";
  }

  /// Create a `ValueId` for an unsigned index of type
  /// `VocabIndex|TextRecordIndex|LocalVocabIndex`. These types can
  /// represent values in the range [0, 2^60]. When `index` is outside of this
  /// range, and `IndexTooLargeException` is thrown.
  static ValueId makeFromVocabIndex(VocabIndex index) {
    return makeFromIndex(index.get(), Datatype::VocabIndex);
  }

  static ValueId makeFromEncodedVal(uint64_t idx) {
    return makeFromIndex(idx, Datatype::EncodedVal);
  }

  static ValueId makeFromTextRecordIndex(TextRecordIndex index) {
    return makeFromIndex(index.get(), Datatype::TextRecordIndex);
  }
  static ValueId makeFromLocalVocabIndex(LocalVocabIndex index) {
    // The last `numDatatypeBits` of a `LocalVocabIndex` are always zero, so we
    // can reuse them for the datatype.
    static_assert(alignof(decltype(*index)) >= (1u << numDatatypeBits));
    return makeFromIndex(reinterpret_cast<T>(index) >> numDatatypeBits,
                         Datatype::LocalVocabIndex);
  }
  static ValueId makeFromWordVocabIndex(WordVocabIndex index) {
    return makeFromIndex(index.get(), Datatype::WordVocabIndex);
  }
  static ValueId makeFromBlankNodeIndex(BlankNodeIndex index) {
    return makeFromIndex(index.get(), Datatype::BlankNodeIndex);
  }

  /// Obtain the unsigned index that this `ValueId` encodes. If `getDatatype()
  /// != [VocabIndex|TextRecordIndex|LocalVocabIndex]` then the result is
  /// unspecified.
  [[nodiscard]] constexpr VocabIndex getVocabIndex() const noexcept {
    return VocabIndex::make(removeDatatypeBits(_bits));
  }

  [[nodiscard]] constexpr uint64_t getEncodedVal() const noexcept {
    return removeDatatypeBits(_bits);
  }

  [[nodiscard]] constexpr TextRecordIndex getTextRecordIndex() const noexcept {
    return TextRecordIndex::make(removeDatatypeBits(_bits));
  }
  [[nodiscard]] LocalVocabIndex getLocalVocabIndex() const noexcept {
    return reinterpret_cast<LocalVocabIndex>(_bits << numDatatypeBits);
  }
  [[nodiscard]] constexpr WordVocabIndex getWordVocabIndex() const noexcept {
    return WordVocabIndex::make(removeDatatypeBits(_bits));
  }

  [[nodiscard]] constexpr BlankNodeIndex getBlankNodeIndex() const noexcept {
    return BlankNodeIndex::make(removeDatatypeBits(_bits));
  }

  // The bit layout of the 60 data bits of a `ViewVocabIndex` (from most to
  // least significant). See the `ValueId` class comment for the semantics.
  static constexpr T numViewIdBits = 12;
  static constexpr T numViewSortHelperBits = 10;
  static constexpr T numViewVocabIndexBits = 38;
  static_assert(numViewIdBits + numViewSortHelperBits + numViewVocabIndexBits ==
                numDataBits);
  // The number of equal-sized parts the main vocabulary is split into for the
  // sorting helper (`2 ** numViewSortHelperBits`).
  static constexpr T numViewSortHelperParts = 1ULL << numViewSortHelperBits;

  // The three parts a `ViewVocabIndex` is decoded into.
  struct ViewVocabIndexParts {
    uint64_t viewId_;
    uint64_t sortHelper_;
    uint64_t index_;
    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ViewVocabIndexParts, viewId_,
                                                sortHelper_, index_)
  };

  // Create a `ValueId` of type `ViewVocabIndex` from its three parts.
  static ValueId makeFromViewVocabIndex(uint64_t viewId, uint64_t sortHelper,
                                        uint64_t index) {
    AD_CONTRACT_CHECK(viewId < (1ULL << numViewIdBits));
    AD_CONTRACT_CHECK(sortHelper < (1ULL << numViewSortHelperBits));
    AD_CONTRACT_CHECK(index < (1ULL << numViewVocabIndexBits));
    T data = (viewId << (numViewSortHelperBits + numViewVocabIndexBits)) |
             (sortHelper << numViewVocabIndexBits) | index;
    return addDatatypeBits(data, Datatype::ViewVocabIndex);
  }

  // The materialized view ID stored in a `ViewVocabIndex`.
  [[nodiscard]] constexpr uint64_t getViewVocabId() const noexcept {
    return removeDatatypeBits(_bits) >>
           (numViewSortHelperBits + numViewVocabIndexBits);
  }
  // The sorting helper stored in a `ViewVocabIndex`.
  [[nodiscard]] constexpr uint64_t getViewVocabSortHelper() const noexcept {
    return (_bits >> numViewVocabIndexBits) &
           ad_utility::bitMaskForLowerBits(numViewSortHelperBits);
  }
  // The index into the view's vocabulary stored in a `ViewVocabIndex`.
  [[nodiscard]] constexpr uint64_t getViewVocabIndexInVocab() const noexcept {
    return _bits & ad_utility::bitMaskForLowerBits(numViewVocabIndexBits);
  }
  // All three parts of a `ViewVocabIndex` at once.
  [[nodiscard]] constexpr ViewVocabIndexParts getViewVocabIndex()
      const noexcept {
    return {getViewVocabId(), getViewVocabSortHelper(),
            getViewVocabIndexInVocab()};
  }

  // Store or load a `Date` object.
  static ValueId makeFromDate(DateYearOrDuration d) noexcept {
    return addDatatypeBits(absl::bit_cast<uint64_t>(d), Datatype::Date);
  }

  DateYearOrDuration getDate() const noexcept {
    return absl::bit_cast<DateYearOrDuration>(removeDatatypeBits(_bits));
  }

  // TODO<joka921> implement dates

  /// Create a `ValueId` for a GeoPoint object (representing a POINT from WKT).
  static ValueId makeFromGeoPoint(GeoPoint p) {
    return addDatatypeBits(p.toBitRepresentation(), Datatype::GeoPoint);
  }

  /// Obtain a new `GeoPoint` object representing the pair of coordinates that
  /// this `ValueId` encodes. If `getDatatype() != GeoPoint` then the result
  /// is unspecified.
  GeoPoint getGeoPoint() const {
    T bits = removeDatatypeBits(_bits);
    return GeoPoint::fromBitRepresentation(bits);
  }

  // An ID is considered trivial, if its datatype is trivial (see
  // `isDatatypeTrivial` above).
  constexpr bool isTrivial() const { return isDatatypeTrivial(getDatatype()); }

  // An `Id` is considered bitwise comparable if the mapping at
  // `isTypeBitwiseComparable_` says so. This is currently the case for all
  // datatypes except for the local vocab index.
  constexpr bool canBeComparedBitwise() const {
    static_assert(onlyLocalAndViewVocabNotBitwiseComparable);
    return isTypeBitwiseComparable_.at(static_cast<size_t>(getDatatype()));
  }

  // Constant to be used in `static_assert` statements to indicate that behavior
  // relies on `LocalVocabIndex` and `ViewVocabIndex` being the only datatypes
  // that are not bitwise comparable. If this ever changes, revisit all callers
  // of `compareWithoutLocalVocab` and `canBeComparedBitwise` (in particular the
  // fast paths during index building, which assume that the IDs they see are
  // bitwise comparable).
  constexpr static bool onlyLocalAndViewVocabNotBitwiseComparable =
      isTypeBitwiseComparable_.size() ==
          static_cast<size_t>(Datatype::MaxValue) + 1 &&
      !isTypeBitwiseComparable_.at(
          static_cast<size_t>(Datatype::LocalVocabIndex)) &&
      !isTypeBitwiseComparable_.at(
          static_cast<size_t>(Datatype::ViewVocabIndex));

  /// Return the smallest and largest possible `ValueId` wrt the underlying
  /// representation
  constexpr static ValueId min() noexcept {
    return {std::numeric_limits<T>::min()};
  }
  constexpr static ValueId max() noexcept {
    return {std::numeric_limits<T>::max()};
  }

  /// Enable hashing in abseil for `ValueId` (required by `ad_utility::HashSet`
  /// and `ad_utility::HashMap`
  template <typename H>
  friend H AbslHashValue(H h, const ValueId& id) {
    // Adding 0/1 to the hash is required to ensure that for two unequal
    // elements the hash expansions of neither is a suffix of the other.This is
    // a property that absl requires for hashes. The hash expansion is the list
    // of simpler values actually being hashed (here: bits or hash expansion of
    // the LocalVocabEntry).
    if (id.getDatatype() == Datatype::ViewVocabIndex) {
      // Hash by the string value with the same tag `1` as a local-vocabulary
      // word that is not in the main vocabulary (see below), so that a
      // `ViewVocabIndex` and such a `LocalVocabIndex` (or another
      // `ViewVocabIndex`, possibly from a different view) that hold the same
      // string hash equally, consistent with `compareThreeWay`.
      return H::combine(std::move(h), viewVocabHooks().viewVocabString(id), 1);
    }
    if (id.getDatatype() != Datatype::LocalVocabIndex) {
      static_assert(onlyLocalAndViewVocabNotBitwiseComparable);
      return H::combine(std::move(h), id._bits, 0);
    }
    auto [lower, upper] = id.getLocalVocabIndex()->positionInVocab();
    if (upper != lower) {
      return H::combine(std::move(h), lower.get(), 0);
    }
    // Not in the main vocabulary: hash by the string representation (rather
    // than the `LocalVocabEntry` object) with tag `1`, so that this is
    // consistent with the hashing of an equal `ViewVocabIndex` above.
    return H::combine(std::move(h),
                      id.getLocalVocabIndex()->toStringRepresentation(), 1);
  }

  /// Enable the serialization of `ValueId` in the `ad_utility::serialization`
  /// framework.
  AD_SERIALIZE_FRIEND_FUNCTION(ValueId) { serializer | arg._bits; }

  /// Similar to `std::visit` for `std::variant`. First gets the datatype and
  /// then calls `visitor(getTYPE)` where `getTYPE` is the correct getter method
  /// for the datatype (e.g. `getDouble` for `Datatype::Double`). Visitor must
  /// be callable with all of the possible return types of the `getTYPE`
  /// functions.
  /// TODO<joka921> This currently still has limited functionality because
  /// VocabIndex, LocalVocabIndex, TextRecordIndex,  and EncodedVal are all of
  /// the same type `uint64_t` and the visitor cannot distinguish between them.
  /// Create strong types for these indices and make the `ValueId` class use
  /// them.
  template <typename Visitor>
  decltype(auto) visit(Visitor&& visitor) const {
    switch (getDatatype()) {
      case Datatype::Undefined:
        return std::invoke(visitor, getUndefined());
      case Datatype::Bool:
        return std::invoke(visitor, getBool());
      case Datatype::Double:
        return std::invoke(visitor, getDouble());
      case Datatype::Int:
        return std::invoke(visitor, getInt());
      case Datatype::EncodedVal:
        return std::invoke(visitor, getEncodedVal());
      case Datatype::VocabIndex:
        return std::invoke(visitor, getVocabIndex());
      case Datatype::LocalVocabIndex:
        return std::invoke(visitor, getLocalVocabIndex());
      case Datatype::TextRecordIndex:
        return std::invoke(visitor, getTextRecordIndex());
      case Datatype::WordVocabIndex:
        return std::invoke(visitor, getWordVocabIndex());
      case Datatype::Date:
        return std::invoke(visitor, getDate());
      case Datatype::GeoPoint:
        return std::invoke(visitor, getGeoPoint());
      case Datatype::BlankNodeIndex:
        return std::invoke(visitor, getBlankNodeIndex());
      case Datatype::ViewVocabIndex:
        return std::invoke(visitor, getViewVocabIndex());
    }
    AD_FAIL();
  }

  /// This operator is only for debugging and testing. It returns a
  /// human-readable representation.
  friend std::ostream& operator<<(std::ostream& ostr, const ValueId& id) {
    ostr << toString(id.getDatatype())[0] << ':';
    if (id.getDatatype() == Datatype::Undefined) {
      return ostr << id.getBits();
    }

    auto visitor = [&ostr](auto&& value) {
      using T = decltype(value);
      if constexpr (ad_utility::isSimilar<T, UndefinedType>) {
        // already handled above
        AD_FAIL();
      } else if constexpr (ad_utility::SimilarToAny<T, double, int64_t,
                                                    uint64_t>) {
        ostr << std::to_string(value);
      } else if constexpr (ad_utility::isSimilar<T, bool>) {
        ostr << (value ? "true" : "false");
      } else if constexpr (ad_utility::isSimilar<T, DateYearOrDuration>) {
        ostr << value.toStringAndType().first;
      } else if constexpr (ad_utility::isSimilar<T, GeoPoint>) {
        ostr << value.toStringRepresentation();
      } else if constexpr (ad_utility::isSimilar<T, LocalVocabIndex>) {
        AD_CORRECTNESS_CHECK(value != nullptr);
        ostr << value->toStringRepresentation();
      } else if constexpr (ad_utility::isSimilar<T, ViewVocabIndexParts>) {
        ostr << "view " << value.viewId_ << ", index " << value.index_;
      } else {
        // T is `VocabIndex | TextRecordIndex`
        ostr << std::to_string(value.get());
      }
    };
    id.visit(visitor);
    return ostr;
  }

 private:
  // Three-way comparison for the case that at least one of the two `ValueId`s
  // is a `ViewVocabIndex`. Uses the sort helper to shortcut where possible and
  // otherwise falls back to a full string comparison via the registered
  // `ViewVocabComparisonHooks`. Defined out-of-line below the class.
  static ql::strong_ordering compareViewVocabInvolved(const ValueId& a,
                                                      const ValueId& b);

  // Access the registered `ViewVocabComparisonHooks`, throwing if none is set.
  // Defined out-of-line below the class.
  static const ViewVocabComparisonHooks& viewVocabHooks();

  // Compares a vocabulary index with a local vocabulary index range.
  static ql::strong_ordering compareVocabAndLocalVocab(
      LocalVocabEntry::IdProxy vocabIndex, ::LocalVocabIndex localVocabIndex) {
    auto [lowerBound, upperBound] = localVocabIndex->positionInVocab();
    if (vocabIndex < lowerBound) {
      return ql::strong_ordering::less;
    } else if (vocabIndex >= upperBound) {
      return ql::strong_ordering::greater;
    } else {
      return ql::strong_ordering::equal;
    }
  }

  // Private constructor that implicitly converts from the underlying
  // representation. Used in the implementation of the static factory methods
  // `Double()`, `Int()` etc.
  constexpr ValueId(T bits) : _bits{bits} {}

  // Set the first 4 bits of `bits` to a 4-bit representation of `type`.
  // Requires that the first four bits of `bits` are all zero.
  static constexpr ValueId addDatatypeBits(T bits, Datatype type) {
    auto mask = static_cast<T>(type) << numDataBits;
    return {bits | mask};
  }

  // Set the datatype bits of `bits` to zero.
  static constexpr T removeDatatypeBits(T bits) noexcept {
    auto mask = ad_utility::bitMaskForLowerBits(numDataBits);
    return bits & mask;
  }

  // Helper function for the implementation of the unsigned index types.
  static constexpr ValueId makeFromIndex(T id, Datatype type) {
    if (id > maxIndex) {
      throw IndexTooLargeException(id);
    }
    return addDatatypeBits(id, type);
  }
};

// _____________________________________________________________________________
inline const ViewVocabComparisonHooks& ValueId::viewVocabHooks() {
  const auto* hooks = getViewVocabComparisonHooks();
  AD_CONTRACT_CHECK(hooks != nullptr,
                    "A `ViewVocabIndex` was compared or hashed, but no "
                    "`ViewVocabComparisonHooks` are registered. This is a bug: "
                    "the `MaterializedViewsManager` must register them when it "
                    "loads a view that has a vocabulary.");
  return *hooks;
}

// _____________________________________________________________________________
inline ql::strong_ordering ValueId::compareViewVocabInvolved(const ValueId& a,
                                                             const ValueId& b) {
  using enum Datatype;
  auto typeA = a.getDatatype();
  auto typeB = b.getDatatype();
  AD_CORRECTNESS_CHECK(typeA == ViewVocabIndex || typeB == ViewVocabIndex);

  // Both sides reference a view vocabulary.
  if (typeA == ViewVocabIndex && typeB == ViewVocabIndex) {
    // Same view: the view vocabulary is sorted, so the in-vocab index already
    // reflects the lexical order exactly.
    if (a.getViewVocabId() == b.getViewVocabId()) {
      return ql::compareThreeWay(a.getViewVocabIndexInVocab(),
                                 b.getViewVocabIndexInVocab());
    }
    // Different views: the sort helper is a monotone function of the lexical
    // order, so different buckets already decide the order.
    if (a.getViewVocabSortHelper() != b.getViewVocabSortHelper()) {
      return ql::compareThreeWay(a.getViewVocabSortHelper(),
                                 b.getViewVocabSortHelper());
    }
    // Same bucket but different views: a full string comparison is necessary.
    return viewVocabHooks().compareByString(a, b);
  }

  // Exactly one side is a `ViewVocabIndex`. Compute the order with the view
  // side first, then invert if the view side was actually `b`.
  bool swapped = typeA != ViewVocabIndex;
  const ValueId& view = swapped ? b : a;
  const ValueId& other = swapped ? a : b;
  // A `ViewVocabIndex` only ever refers to a word that is neither in the main
  // vocabulary nor encodable as an IRI: the writer stores those cheaper
  // representations directly as `VocabIndex`/`EncodedVal`. Such a word
  // therefore sorts among the `VocabIndex` values (the lexically ordered main
  // vocabulary), which QLever orders entirely before the `EncodedVal` values
  // and in the "string region" between the numeric and the remaining datatypes.
  // The cases below keep `ViewVocabIndex` consistent with that placement.
  auto order = [&view, &other]() -> ql::strong_ordering {
    switch (other.getDatatype()) {
      // Main-vocabulary word: shortcut via the bucket, and only compare strings
      // within the same bucket.
      case VocabIndex: {
        auto viewBucket = view.getViewVocabSortHelper();
        auto otherBucket = viewVocabHooks().mainVocabBucket(other);
        if (viewBucket != otherBucket) {
          return ql::compareThreeWay(viewBucket, otherBucket);
        }
        return viewVocabHooks().compareByString(view, other);
      }
      // Encoded IRI: a `ViewVocabIndex` (a main-vocabulary-space word) is
      // always less than an `EncodedVal`, consistent with the `VocabIndex`-vs-
      // `EncodedVal` order.
      case EncodedVal:
        return ql::strong_ordering::less;
      // Local-vocabulary word (also a string): compare lexically.
      case LocalVocabIndex:
        return viewVocabHooks().compareByString(view, other);
      // Non-string type. A `ViewVocabIndex` must sort in the same position as
      // the other string types (`VocabIndex`/`LocalVocabIndex`) relative to
      // non-string datatypes: after the numeric/boolean/undefined types and
      // before the rest. We must NOT use `ViewVocabIndex`'s own (highest)
      // datatype bits here, as that would place it inconsistently (e.g. after
      // `GeoPoint`, which a `VocabIndex` sorts before).
      default:
        return other.getDatatype() < minStringType_
                   ? ql::strong_ordering::greater
                   : ql::strong_ordering::less;
    }
  }();
  return swapped ? ql::compareThreeWay(0, order) : order;
}

#endif  // QLEVER_SRC_GLOBAL_VALUEID_H
