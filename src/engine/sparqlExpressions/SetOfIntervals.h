// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SETOFINTERVALS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SETOFINTERVALS_H

#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/VectorWithMemoryLimit.h"

namespace ad_utility {

// A vector of pairs of <size_t, size_t> with the following semantics: It
// represents the union of the intervals [first, second) of the individual
// pairs. The intervals have to be pairwise disjoint and nonempty. They
// also have to be sorted in ascending order. The set additionally stores the
// `size` of the range [0, size) it refers to (typically the size of an
// `EvaluationContext`), all the intervals lie within this range.
//
// When used as the result of a boolean expression, the elements of the set are
// the rows that are `true`. The rows for which the expression is an error
// (`UNDEF` in QLever) are stored separately in `undefIntervals()` (with the
// same invariants, and disjoint from the set), all other rows are `false`.
// `Complement`, `Intersection`, and `Union` implement the three-valued logic of
// `!`, `&&`, and `||` from the SPARQL standard.
struct SetOfIntervals {
  using Vec = std::vector<std::pair<size_t, size_t>>;
  Vec _intervals;

 private:
  size_t size_;
  Vec undefIntervals_;

 public:
  // Construct from the `intervals` and the `size` of the range [0, size) the
  // set refers to, and optionally the `undefIntervals` (see above). Throw if
  // one of the intervals ends after `size`.
  SetOfIntervals(Vec intervals, size_t size, Vec undefIntervals = {});

  // Return the `size` of the range [0, size) this set refers to.
  size_t size() const { return size_; }

  // Return the rows that are neither `true` nor `false` (see above).
  const Vec& undefIntervals() const { return undefIntervals_; }

  // _________________________________________________________________________
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(SetOfIntervals, _intervals, size_,
                                              undefIntervals_)

  // Sort the intervals in ascending order and assert that they are indeed
  // disjoint and nonempty.
  static SetOfIntervals SortAndCheckDisjointAndNonempty(SetOfIntervals input);

  // Assert that the set is sorted, and simplify it by merging adjacent
  // intervals.
  static SetOfIntervals CheckSortedAndDisjointAndSimplify(
      const SetOfIntervals& input);

  // Compute the intersection of two sets of intervals, which must refer to
  // the same range. A row is `undef` in the result iff it is `undef` in one of
  // the inputs and not `false` in the other.
  struct Intersection {
    SetOfIntervals operator()(SetOfIntervals A, SetOfIntervals B) const;
  };

  // Compute the union of two sets of intervals, which must refer to the same
  // range. A row is `undef` in the result iff it is `undef` in one of the
  // inputs and not `true` in the other.
  struct Union {
    SetOfIntervals operator()(SetOfIntervals A, SetOfIntervals B) const;
  };

  // Compute the complement of a set of intervals within the range the set
  // refers to. The `undef` rows stay `undef`.
  struct Complement {
    SetOfIntervals operator()(SetOfIntervals s) const;
  };

  // Transform a SetOfIntervals to a std::vector<bool> of size `s.size()`
  // where the element at index i is true if and only if i is contained in the
  // set (so the `undef` rows are `false`).
  // __________________________________________________________________________
  inline static std::vector<bool> toBitVector(const SetOfIntervals& s) {
    std::vector<bool> result(s.size(), false);
    for (const auto& [begin, end] : s._intervals) {
      std::fill(result.begin() + begin, result.begin() + end, true);
    }
    return result;
  }

  // Transform a `SetOfIntervals` into a vector of `Id`s of size `set.size()`.
  // The i-th element is true iff i is contained in the set, undefined iff i is
  // contained in the `undefIntervals()`, and false otherwise.
  inline static VectorWithMemoryLimit<Id> toIdVector(
      const SetOfIntervals& set,
      const VectorWithMemoryLimit<Id>::Allocator& allocator) {
    VectorWithMemoryLimit<Id> result{allocator};
    result.resize(set.size(), Id::makeFromBool(false));
    auto fill = [&result](const Vec& intervals, Id id) {
      for (const auto& [begin, end] : intervals) {
        std::fill(result.begin() + begin, result.begin() + end, id);
      }
    };
    fill(set._intervals, Id::makeFromBool(true));
    fill(set.undefIntervals_, Id::makeUndefined());
    return result;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SETOFINTERVALS_H
