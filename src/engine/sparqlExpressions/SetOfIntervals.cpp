// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "engine/sparqlExpressions/SetOfIntervals.h"

#include "backports/algorithm.h"

namespace ad_utility {
// ___________________________________________________________________________
SetOfIntervals::SetOfIntervals(Vec intervals, size_t size, Vec undefIntervals)
    : _intervals{std::move(intervals)},
      size_{size},
      undefIntervals_{std::move(undefIntervals)} {
  auto endsInRange = [size](const auto& interval) {
    return interval.second <= size;
  };
  AD_CONTRACT_CHECK(ql::ranges::all_of(_intervals, endsInRange) &&
                        ql::ranges::all_of(undefIntervals_, endsInRange),
                    "An interval of a `SetOfIntervals` ends after the size of "
                    "the set.");
}

// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::SortAndCheckDisjointAndNonempty(
    SetOfIntervals input) {
  auto& vec = input._intervals;
  auto cmp = [](const auto& a, const auto& b) { return a.first < b.first; };
  std::sort(vec.begin(), vec.end(), cmp);
  for (size_t i = 0; i < vec.size(); ++i) {
    AD_CONTRACT_CHECK(vec[i].second > vec[i].first);
  }

  for (size_t i = 1; i < vec.size(); ++i) {
    AD_CONTRACT_CHECK(vec[i].first >= vec[i - 1].second);
  }

  return input;
}

namespace {
// The intersection of the `_intervals` of `A` and `B`, ignoring their
// `undefIntervals()`.
SetOfIntervals intersectionIgnoringUndef(SetOfIntervals A, SetOfIntervals B) {
  AD_CONTRACT_CHECK(A.size() == B.size());
  // First sort by the beginning of the interval
  A = SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(A));
  B = SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(B));

  SetOfIntervals result{{}, A.size()};
  auto itA = A._intervals.begin();
  auto itB = B._intervals.begin();

  // All values smaller than minIdxNotChecked are either already contained in
  // the result or will never become part of it. This variable helps us to
  // enforce the invariant that the result intervals are disjoint.
  size_t minIdxNotChecked = 0;

  // Compute the intersection using the "zipper" algorithm extended to
  // intervals.
  while (itA < A._intervals.end() && itB < B._intervals.end()) {
    // Invariant: All intervals before iteratorA and iteratorB have already been
    // completely dealt with.
    auto& itSmaller = itA->first < itB->first ? itA : itB;
    auto& itGreaterEq = itA->first < itB->first ? itB : itA;

    // Compute the intersection.
    std::pair<size_t, size_t> intersection{
        itGreaterEq->first, std::min(itGreaterEq->second, itSmaller->second)};

    // Truncate the intersection s.t. it lies completely after (including)
    // minIdxNotChecked. Also update minIdxNotChecked, which is then guaranteed
    // to be >= std::min(itGreaterEq->second, itSmaller->second)
    intersection.first = std::max(intersection.first, minIdxNotChecked);
    minIdxNotChecked = std::max(minIdxNotChecked, intersection.second);

    if (intersection.first < intersection.second) {
      result._intervals.push_back(std::move(intersection));
    }

    // At least one of the iterators is advanced, which guarantees progress.
    if (minIdxNotChecked >= itSmaller->second) {
      ++itSmaller;
    }
    if (minIdxNotChecked >= itGreaterEq->second) {
      ++itGreaterEq;
    }
  }

  return SetOfIntervals::CheckSortedAndDisjointAndSimplify(
      SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(result)));
}

// The union of the `_intervals` of `A` and `B`, ignoring their
// `undefIntervals()`.
SetOfIntervals unionIgnoringUndef(SetOfIntervals A, SetOfIntervals B) {
  AD_CONTRACT_CHECK(A.size() == B.size());
  // First sort by the beginning of the interval
  A = SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(A));
  B = SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(B));
  SetOfIntervals result{{}, A.size()};
  auto itA = A._intervals.begin();
  auto itB = B._intervals.begin();

  // All values smaller than minIdxNotChecked are either already contained in
  // the result or will never become part of it. This variable helps us to
  // enforce the invariant that the result intervals are disjoint.
  size_t minIdxNotChecked = 0;

  // Truncate an interval such that it lies after (including) minIdxNotChecked.
  // Update minIdxNotChecked and append the interval to the result, if it did
  // not become empty by the truncation.
  auto truncateAndAppendInterval =
      [&minIdxNotChecked, &result](std::pair<size_t, size_t> interval) {
        interval.first = std::max(minIdxNotChecked, interval.first);
        minIdxNotChecked = std::max(minIdxNotChecked, interval.second);

        if (interval.first < interval.second) {
          result._intervals.push_back(interval);
        }
      };

  // Compute the union using the "zipper" algorithm extended to
  // intervals.
  while (itA < A._intervals.end() && itB < B._intervals.end()) {
    auto& itSmaller = itA->first < itB->first ? itA : itB;
    auto& itGreaterEq = itA->first < itB->first ? itB : itA;

    // If the intervals do not overlap, output the smaller one (unless
    // minIdxNotChecked >= the right end of the interval.
    if (itSmaller->second <= itGreaterEq->first) {
      truncateAndAppendInterval(*itSmaller);
      ++itSmaller;
      continue;
    }
    // The ranges overlap
    std::pair<size_t, size_t> nextUnion{
        itSmaller->first, std::max(itGreaterEq->second, itSmaller->second)};
    truncateAndAppendInterval(nextUnion);
    ;
    ++itSmaller;
    ++itGreaterEq;
  }

  // Attach the remaining intervals (which at this point either all come from A
  // or from B)
  std::for_each(itA, A._intervals.end(), truncateAndAppendInterval);
  std::for_each(itB, B._intervals.end(), truncateAndAppendInterval);

  return SetOfIntervals::CheckSortedAndDisjointAndSimplify(
      SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(result)));
}

// The complement of the `_intervals` of `s`, ignoring its `undefIntervals()`.
SetOfIntervals complementIgnoringUndef(SetOfIntervals s) {
  s = SetOfIntervals::SortAndCheckDisjointAndNonempty(std::move(s));
  SetOfIntervals result{{}, s.size()};
  auto& intervals = result._intervals;

  size_t lastElement = 0;
  for (auto [begin, end] : s._intervals) {
    // The range that was previously false (not part of the set), now becomes
    // true.
    if (lastElement < begin) {
      intervals.emplace_back(lastElement, begin);
    }
    lastElement = end;
  }
  if (lastElement < s.size()) {
    intervals.emplace_back(lastElement, s.size());
  }
  return result;
}

// The rows of `s` that are `true` resp. `undef` resp. `false`, each as a
// `SetOfIntervals` without `undefIntervals()`.
SetOfIntervals trueRows(const SetOfIntervals& s) {
  return {s._intervals, s.size()};
}
SetOfIntervals undefRows(const SetOfIntervals& s) {
  return {s.undefIntervals(), s.size()};
}
SetOfIntervals falseRows(const SetOfIntervals& s) {
  return complementIgnoringUndef(unionIgnoringUndef(trueRows(s), undefRows(s)));
}

// Combine the `trueRows` and `falseRows` of a result, all other rows are
// `undef`.
SetOfIntervals fromTrueAndFalseRows(SetOfIntervals trueRows,
                                    const SetOfIntervals& falseRows) {
  auto undef = complementIgnoringUndef(unionIgnoringUndef(trueRows, falseRows));
  return {std::move(trueRows._intervals), trueRows.size(),
          std::move(undef._intervals)};
}
}  // namespace

// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::Intersection::operator()(
    SetOfIntervals A, SetOfIntervals B) const {
  if (A.undefIntervals_.empty() && B.undefIntervals_.empty()) {
    return intersectionIgnoringUndef(std::move(A), std::move(B));
  }
  // `false && undef` is `false`, `true && undef` is `undef`.
  return fromTrueAndFalseRows(intersectionIgnoringUndef(A, B),
                              unionIgnoringUndef(falseRows(A), falseRows(B)));
}

// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::Union::operator()(SetOfIntervals A,
                                                 SetOfIntervals B) const {
  if (A.undefIntervals_.empty() && B.undefIntervals_.empty()) {
    return unionIgnoringUndef(std::move(A), std::move(B));
  }
  // `true || undef` is `true`, `false || undef` is `undef`.
  return fromTrueAndFalseRows(
      unionIgnoringUndef(A, B),
      intersectionIgnoringUndef(falseRows(A), falseRows(B)));
}

// ___________________________________________________________________________
SetOfIntervals SetOfIntervals::CheckSortedAndDisjointAndSimplify(
    const SetOfIntervals& inputSet) {
  auto& inputVec = inputSet._intervals;
  SetOfIntervals result{{}, inputSet.size(), inputSet.undefIntervals_};
  if (inputVec.empty()) {
    return result;
  }
  auto current = inputVec[0];
  for (size_t i = 1; i < inputVec.size(); ++i) {
    AD_CONTRACT_CHECK(inputVec[i].first >= current.second);
    if (inputVec[i].first == current.second) {
      current = {current.first, inputVec[i].second};
    } else {
      result._intervals.push_back(current);
      current = inputVec[i];
    }
  }
  result._intervals.push_back(current);
  return result;
}

// ____________________________________________________________________________
SetOfIntervals SetOfIntervals::Complement::operator()(SetOfIntervals s) const {
  if (s.undefIntervals_.empty()) {
    return complementIgnoringUndef(std::move(s));
  }
  // `!undef` is `undef`, so the `false` rows become `true`.
  auto result = falseRows(s);
  result.undefIntervals_ = std::move(s.undefIntervals_);
  return result;
}

}  // namespace ad_utility
