//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <bitset>

#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "global/ValueId.h"
#include "index/EncodedIriManager.h"
#include "index/LocalVocabEntry.h"
#include "util/HashSet.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

struct ValueIdTest : public ::testing::Test {
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};

TEST_F(ValueIdTest, makeFromDouble) {
  auto testRepresentableDouble = [](double d) {
    auto id = ValueId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // We lose `numDatatypeBits` bits of precision, so `ASSERT_DOUBLE_EQ` would
    // fail.
    ASSERT_FLOAT_EQ(id.getDouble(), d);
    // This check expresses the precision more exactly
    if (id.getDouble() != d) {
      // The if is needed for the case of += infinity.
      ASSERT_NEAR(
          id.getDouble(), d,
          std::abs(d / (uint64_t{1} << (52 - ValueId::numDatatypeBits))));
    }
  };

  auto testNonRepresentableSubnormal = [](double d) {
    auto id = ValueId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // Subnormal numbers with a too small fraction are rounded to zero.
    ASSERT_EQ(id.getDouble(), 0.0);
  };
  for (size_t i = 0; i < 10'000; ++i) {
    testRepresentableDouble(positiveRepresentableDoubleGenerator());
    testRepresentableDouble(negativeRepresentableDoubleGenerator());
    auto nonRepresentable = nonRepresentableDoubleGenerator();
    // The random number generator includes the edge cases which would make the
    // tests fail.
    if (nonRepresentable != ValueId::minPositiveDouble &&
        nonRepresentable != -ValueId::minPositiveDouble) {
      testNonRepresentableSubnormal(nonRepresentable);
    }
  }

  testRepresentableDouble(std::numeric_limits<double>::infinity());
  testRepresentableDouble(-std::numeric_limits<double>::infinity());

  // Test positive and negative 0.
  ASSERT_NE(absl::bit_cast<uint64_t>(0.0), absl::bit_cast<uint64_t>(-0.0));
  ASSERT_EQ(0.0, -0.0);
  testRepresentableDouble(0.0);
  testRepresentableDouble(-0.0);
  testNonRepresentableSubnormal(0.0);
  testNonRepresentableSubnormal(0.0);

  auto quietNan = std::numeric_limits<double>::quiet_NaN();
  auto signalingNan = std::numeric_limits<double>::signaling_NaN();
  ASSERT_TRUE(std::isnan(ValueId::makeFromDouble(quietNan).getDouble()));
  ASSERT_TRUE(std::isnan(ValueId::makeFromDouble(signalingNan).getDouble()));

  // Test that the value of `minPositiveDouble` is correct.
  auto testSmallestNumber = [](double d) {
    ASSERT_EQ(ValueId::makeFromDouble(d).getDouble(), d);
    ASSERT_NE(d / 2, 0.0);
    ASSERT_EQ(ValueId::makeFromDouble(d / 2).getDouble(), 0.0);
  };
  testSmallestNumber(ValueId::minPositiveDouble);
  testSmallestNumber(-ValueId::minPositiveDouble);
}

TEST_F(ValueIdTest, makeFromInt) {
  for (size_t i = 0; i < 10'000; ++i) {
    auto value = nonOverflowingNBitGenerator();
    auto id = ValueId::makeFromInt(value);
    ASSERT_EQ(id.getDatatype(), Datatype::Int);
    ASSERT_EQ(id.getInt(), value);
  }

  auto testOverflow = [](auto generator) {
    using I = ValueId::IntegerType;
    for (size_t i = 0; i < 10'000; ++i) {
      auto value = generator();
      auto id = ValueId::makeFromInt(value);
      ASSERT_EQ(id.getDatatype(), Datatype::Int);
      ASSERT_EQ(id.getInt(), I::fromNBit(I::toNBit(value)));
      ASSERT_NE(id.getInt(), value);
    }
  };

  testOverflow(overflowingNBitGenerator);
  testOverflow(underflowingNBitGenerator);
}

// _____________________________________________________________________________
TEST_F(ValueIdTest, makeFromBool) {
  EXPECT_TRUE(ValueId::makeBoolFromZeroOrOne(true).getBool());
  EXPECT_TRUE(ValueId::makeFromBool(true).getBool());
  EXPECT_FALSE(ValueId::makeBoolFromZeroOrOne(false).getBool());
  EXPECT_FALSE(ValueId::makeFromBool(false).getBool());

  EXPECT_EQ(ValueId::makeBoolFromZeroOrOne(true).getBoolLiteral(), "1");
  EXPECT_EQ(ValueId::makeFromBool(true).getBoolLiteral(), "true");
  EXPECT_EQ(ValueId::makeBoolFromZeroOrOne(false).getBoolLiteral(), "0");
  EXPECT_EQ(ValueId::makeFromBool(false).getBoolLiteral(), "false");
}

TEST_F(ValueIdTest, Indices) {
  auto testRandomIds = [&](auto makeId, auto getFromId, Datatype type) {
    auto testSingle = [&](auto value) {
      auto id = makeId(value);
      ASSERT_EQ(id.getDatatype(), type);
      ASSERT_EQ(std::invoke(getFromId, id), value);
    };
    for (size_t idx = 0; idx < 10'000; ++idx) {
      testSingle(indexGenerator());
    }
    testSingle(0);
    testSingle(ValueId::maxIndex);

    if (type != Datatype::LocalVocabIndex) {
      for (size_t idx = 0; idx < 10'000; ++idx) {
        auto value = invalidIndexGenerator();
        ASSERT_THROW(makeId(value), ValueId::IndexTooLargeException);
        AD_EXPECT_THROW_WITH_MESSAGE(
            makeId(value), ::testing::ContainsRegex("is bigger than"));
      }
    }
  };

  testRandomIds(&makeTextRecordId, &getTextRecordIndex,
                Datatype::TextRecordIndex);
  testRandomIds(&makeVocabId, &getVocabIndex, Datatype::VocabIndex);

  auto localVocabWordToInt = [](const auto& input) {
    return std::atoll(getLocalVocabIndex(input).c_str());
  };
  testRandomIds(&makeLocalVocabId, localVocabWordToInt,
                Datatype::LocalVocabIndex);
  testRandomIds(&makeWordVocabId, &getWordVocabIndex, Datatype::WordVocabIndex);
}

TEST_F(ValueIdTest, Undefined) {
  auto id = ValueId::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);
}

TEST_F(ValueIdTest, OrderingDifferentDatatypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto compareByDatatypeAndIndexTypes = [](ValueId a, ValueId b) {
    auto typeA = a.getDatatype();
    auto typeB = b.getDatatype();
    if (ad_utility::contains(ValueId::stringTypes_, typeA) &&
        ad_utility::contains(ValueId::stringTypes_, typeB)) {
      return false;
    }
    return a.getDatatype() < b.getDatatype();
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), compareByDatatypeAndIndexTypes));
}

TEST_F(ValueIdTest, IndexOrdering) {
  auto testOrder = [](auto makeIdFromIndex, auto getIndexFromId) {
    std::vector<ValueId> ids;
    addIdsFromGenerator(indexGenerator, makeIdFromIndex, ids);
    std::vector<std::invoke_result_t<decltype(getIndexFromId), ValueId>>
        indices;
    for (auto id : ids) {
      indices.push_back(std::invoke(getIndexFromId, id));
    }

    std::sort(ids.begin(), ids.end());
    std::sort(indices.begin(), indices.end());

    for (size_t i = 0; i < ids.size(); ++i) {
      ASSERT_EQ(std::invoke(getIndexFromId, ids[i]), indices[i]);
    }
  };

  testOrder(&makeVocabId, &getVocabIndex);
  testOrder(&makeLocalVocabId, &getLocalVocabIndex);
  testOrder(&makeWordVocabId, &getWordVocabIndex);
  testOrder(&makeTextRecordId, &getTextRecordIndex);
}

TEST_F(ValueIdTest, DoubleOrdering) {
  auto ids = makeRandomDoubleIds();
  std::vector<double> doubles;
  doubles.reserve(ids.size());
  for (auto id : ids) {
    doubles.push_back(id.getDouble());
  }
  std::sort(ids.begin(), ids.end());

  // The sorting of `double`s is broken as soon as NaNs are present. We remove
  // the NaNs from the `double`s.
  ql::erase_if(doubles, [](double d) { return std::isnan(d); });
  std::sort(doubles.begin(), doubles.end());

  // When sorting ValueIds that hold doubles, the NaN values form a contiguous
  // range.
  auto beginOfNans = std::find_if(ids.begin(), ids.end(), [](const auto& id) {
    return std::isnan(id.getDouble());
  });
  auto endOfNans = std::find_if(ids.rbegin(), ids.rend(), [](const auto& id) {
                     return std::isnan(id.getDouble());
                   }).base();
  for (auto it = beginOfNans; it < endOfNans; ++it) {
    ASSERT_TRUE(std::isnan(it->getDouble()));
  }

  // The NaN values are sorted directly after positive infinity.
  ASSERT_EQ((beginOfNans - 1)->getDouble(),
            std::numeric_limits<double>::infinity());
  // Delete the NaN values without changing the order of all other types.
  ids.erase(beginOfNans, endOfNans);

  // In `ids` the negative number stand AFTER the positive numbers because of
  // the bitOrdering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](ValueId id) {
    auto bits = absl::bit_cast<uint64_t>(id.getDouble());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };
  auto beginOfNegatives =
      std::find_if(ids.begin(), ids.end(), doubleIdIsNegative);
  auto endOfNegatives = std::rotate(ids.begin(), beginOfNegatives, ids.end());

  // The negative numbers now come before the positive numbers, but the are
  // ordered in descending instead of ascending order, reverse them.
  std::reverse(ids.begin(), endOfNegatives);

  // After these two transformations (switch positive and negative range,
  // reverse negative range) the `ids` are sorted in exactly the same order as
  // the `doubles`.
  for (size_t i = 0; i < ids.size(); ++i) {
    auto doubleTruncated = ValueId::makeFromDouble(doubles[i]).getDouble();
    ASSERT_EQ(ids[i].getDouble(), doubleTruncated);
  }
}

TEST_F(ValueIdTest, SignedIntegerOrdering) {
  std::vector<ValueId> ids;
  addIdsFromGenerator(nonOverflowingNBitGenerator, &ValueId::makeFromInt, ids);
  std::vector<int64_t> integers;
  integers.reserve(ids.size());
  for (auto id : ids) {
    integers.push_back(id.getInt());
  }

  std::sort(ids.begin(), ids.end());
  std::sort(integers.begin(), integers.end());

  // The negative integers stand after the positive integers, so we have to
  // switch these ranges.
  auto beginOfNegative = std::find_if(
      ids.begin(), ids.end(), [](ValueId id) { return id.getInt() < 0; });
  std::rotate(ids.begin(), beginOfNegative, ids.end());

  // Now `integers` and `ids` should be in the same order
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(ids[i].getInt(), integers[i]);
  }
}

TEST_F(ValueIdTest, Serialization) {
  auto ids = makeRandomIds();

  for (auto id : ids) {
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    ad_utility::serialization::ByteBufferReadSerializer reader{
        std::move(writer).data()};
    ValueId serializedId;
    reader >> serializedId;
    ASSERT_EQ(id, serializedId);
  }
}

TEST_F(ValueIdTest, Hashing) {
  {
    auto ids = makeRandomIds();
    ad_utility::HashSet<ValueId> idsWithoutDuplicates;
    for (size_t i = 0; i < 2; ++i) {
      for (auto id : ids) {
        idsWithoutDuplicates.insert(id);
      }
    }
    std::vector<ValueId> idsWithoutDuplicatesAsVector(
        idsWithoutDuplicates.begin(), idsWithoutDuplicates.end());

    std::sort(idsWithoutDuplicatesAsVector.begin(),
              idsWithoutDuplicatesAsVector.end());
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    ASSERT_EQ(ids, idsWithoutDuplicatesAsVector);
  }
  {
    using namespace ad_utility::triple_component;
    using namespace ad_utility::testing;
    const Index& index = qec_->getIndex();
    auto mkId = makeGetId(index);
    LocalVocab lv1;
    LocalVocab lv2;
    Iri iri = Iri::fromIriref("<foo>");
    LocalVocabEntry lve1(iri, index);
    LocalVocabEntry lve2(iri, index);
    LocalVocabEntry lve3 =
        LocalVocabEntry::fromStringRepresentation("\"foo\"", index);
    LocalVocabEntry lve4 = LocalVocabEntry::fromIriref("<x>", index);
    auto LVID = [](LocalVocabEntry& lve, LocalVocab& lv) {
      return Id::makeFromLocalVocabIndex(lv.getIndexAndAddIfNotContained(lve));
    };
    // Checks that hashing is implemented correctly using `==` for equality. The
    // hash expansion is the values added with `combine`.
    // - If two elements are equal, then their hash expansions must be equal.
    // - If two elements are not equal, then hash expansions must differ and
    // neither can be a suffix of the other.
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(
        {LVID(lve1, lv1), LVID(lve2, lv2), LVID(lve3, lv1), LVID(lve4, lv1),
         mkId("<x>"), Id::makeFromInt(0), Id::makeFromInt(42),
         Id::makeFromDouble(0), Id::makeFromDouble(1.56),
         Id::makeFromDouble(1e-10), Id::makeFromDouble(1e+100),
         Id::makeFromBool(true), Id::makeFromBool(false),
         Id::makeUndefined()}));
  }
}

TEST_F(ValueIdTest, toDebugString) {
  auto test = [](ValueId id, std::string_view expected) {
    std::stringstream stream;
    stream << id;
    ASSERT_EQ(stream.str(), expected);
  };
  test(ValueId::makeUndefined(), "U:0");
  // Values with type undefined can usually only have one value (all data bits
  // zero). Sometimes ValueIds with type undefined but non-zero data bits are
  // used. The following test tests one of these internal ValueIds.
  ValueId customUndefined = ValueId::fromBits(
      ValueId::IntegerType::fromNBit(100) |
      (static_cast<ValueId::T>(Datatype::Undefined) << ValueId::numDataBits));
  test(customUndefined, "U:100");
  test(ValueId::makeFromDouble(42.0), "D:42.000000");
  test(ValueId::makeFromBool(false), "B:false");
  test(ValueId::makeFromBool(true), "B:true");
  test(ValueId::makeBoolFromZeroOrOne(false), "B:false");
  test(ValueId::makeBoolFromZeroOrOne(true), "B:true");
  test(makeVocabId(15), "V:15");
  auto str = LocalVocabEntry::literalWithoutQuotes(
      "SomeValue", qec_->getLocalVocabContext());
  test(ValueId::makeFromLocalVocabIndex(&str), "L:\"SomeValue\"");
  test(makeTextRecordId(37), "T:37");
  test(makeWordVocabId(42), "W:42");
  test(makeBlankNodeId(27), "B:27");
  test(ValueId::makeFromDate(
           DateYearOrDuration{123456, DateYearOrDuration::Type::Year}),
       "D:123456");
  test(ValueId::makeFromGeoPoint(GeoPoint{50.0, 50.0}),
       "G:POINT(50.000000 50.000000)");
  // make an ID with an invalid datatype
  ASSERT_ANY_THROW(test(ValueId::max(), "blim"));
}

TEST_F(ValueIdTest, InvalidDatatypeEnumValue) {
  ASSERT_ANY_THROW(toString(static_cast<Datatype>(2345)));
}

TEST_F(ValueIdTest, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<ValueId>);
}

// _____________________________________________________________________________
TEST_F(ValueIdTest, EncodedIriEqualityWithLocalVocabEntry) {
  // Test that an ID storing an encoded IRI compares equal to a LocalVocabEntry
  // with the same IRI value.

  // Create an EncodedIriManager with some test prefixes
  std::vector<std::string> prefixes = {"http://example.org/",
                                       "http://test.com/"};

  // Create a test index config with the encoded IRI manager and call getQec
  // to set up the global index state
  using namespace ad_utility::testing;
  TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = prefixes;
  qec_ = getQec(config);
  const auto& encodedIriManager = qec_->getIndex().encodedIriManager();

  // Test case 1: IRI that can be encoded
  std::string encodableIri = "<http://example.org/123>";
  auto encodedIdOpt = encodedIriManager.encode(encodableIri);
  ASSERT_TRUE(encodedIdOpt.has_value())
      << "Failed to encode IRI: " << encodableIri;

  auto encodedId = *encodedIdOpt;
  EXPECT_EQ(encodedId.getDatatype(), Datatype::EncodedVal);

  // Create a LocalVocabEntry with the same IRI
  auto iri = ad_utility::triple_component::Iri::fromIriref(encodableIri);
  LocalVocabEntry localVocabEntry{iri, qec_->getLocalVocabContext()};
  auto localVocabId = ValueId::makeFromLocalVocabIndex(&localVocabEntry);

  // The encoded ID should compare equal to the LocalVocabEntry ID
  EXPECT_EQ(encodedId, localVocabId)
      << "Encoded ID should equal LocalVocabEntry ID for IRI: " << encodableIri;

  // Test case 2: Another encodable IRI with different prefix
  std::string encodableIri2 = "<http://test.com/456>";
  auto encodedIdOpt2 = encodedIriManager.encode(encodableIri2);
  ASSERT_TRUE(encodedIdOpt2.has_value())
      << "Failed to encode IRI: " << encodableIri2;

  auto encodedId2 = *encodedIdOpt2;
  auto iri2 = ad_utility::triple_component::Iri::fromIriref(encodableIri2);
  LocalVocabEntry localVocabEntry2{iri2, qec_->getLocalVocabContext()};
  auto localVocabId2 = ValueId::makeFromLocalVocabIndex(&localVocabEntry2);

  EXPECT_EQ(encodedId2, localVocabId2)
      << "Encoded ID should equal LocalVocabEntry ID for IRI: "
      << encodableIri2;

  // Test case 3: Encoded IDs should not equal LocalVocabEntries with different
  // IRIs
  EXPECT_NE(encodedId, localVocabId2)
      << "Different encoded IRIs should not be equal";
  EXPECT_NE(encodedId2, localVocabId)
      << "Different encoded IRIs should not be equal";

  // Test case 4: Ordering should also work correctly

  auto inconsistentOrderingMessage =
      "Ordering should be consistent between encoded and local vocab IDs";
  if (encodableIri < encodableIri2) {
    EXPECT_LT(encodedId, localVocabId2) << inconsistentOrderingMessage;
    EXPECT_GT(localVocabId2, encodedId) << inconsistentOrderingMessage;
  } else {
    EXPECT_GT(encodedId, localVocabId2) << inconsistentOrderingMessage;
    EXPECT_LT(localVocabId2, encodedId) << inconsistentOrderingMessage;
  }
}

// Note: the `isTrivial` functionality is also tested using `static_assert`s
// across the codebase, hence we don't test it exhaustively here, but only
// please the coverage tool.
TEST(ValueId, isTrivial) {
  EXPECT_TRUE(Id::makeUndefined().isTrivial());
  EXPECT_FALSE(
      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(17)).isTrivial());
  EXPECT_FALSE(Id::makeFromEncodedVal(738).isTrivial());
}

// _____________________________________________________________________________
TEST(ValueId, canBeComparedBitwise) {
  EXPECT_TRUE(Id::makeUndefined().canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromBool(true).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromInt(1337).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromDouble(3.14).canBeComparedBitwise());
  EXPECT_TRUE(
      Id::makeFromVocabIndex(VocabIndex::make(0)).canBeComparedBitwise());
  EXPECT_FALSE(Id::makeFromLocalVocabIndex(nullptr).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromTextRecordIndex(TextRecordIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromDate(DateYearOrDuration{Date{0, 0, 0}})
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromGeoPoint(GeoPoint{0, 0}).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromWordVocabIndex(WordVocabIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromEncodedVal(738).canBeComparedBitwise());
  EXPECT_FALSE(Id::makeFromViewVocabIndex(0, 0, 0).canBeComparedBitwise());
}

// _____________________________________________________________________________
TEST(ValueId, ViewVocabIndexBitLayout) {
  // Round-trip of the three parts through the bit layout.
  auto check = [](uint64_t viewId, uint64_t sortHelper, uint64_t index) {
    auto id = Id::makeFromViewVocabIndex(viewId, sortHelper, index);
    EXPECT_EQ(id.getDatatype(), Datatype::ViewVocabIndex);
    EXPECT_EQ(id.getViewVocabId(), viewId);
    EXPECT_EQ(id.getViewVocabSortHelper(), sortHelper);
    EXPECT_EQ(id.getViewVocabIndexInVocab(), index);
    EXPECT_EQ(id.getViewVocabIndex(),
              (Id::ViewVocabIndexParts{viewId, sortHelper, index}));
  };
  check(0, 0, 0);
  check(1, 2, 3);
  // Maximum value in each field (12, 10, 38 bits respectively). The parts must
  // not bleed into each other.
  constexpr uint64_t maxViewId = (1ULL << Id::numViewIdBits) - 1;
  constexpr uint64_t maxSortHelper = (1ULL << Id::numViewSortHelperBits) - 1;
  constexpr uint64_t maxIndex = (1ULL << Id::numViewVocabIndexBits) - 1;
  check(maxViewId, maxSortHelper, maxIndex);
  check(maxViewId, 0, 0);
  check(0, maxSortHelper, 0);
  check(0, 0, maxIndex);

  EXPECT_EQ(toString(Datatype::ViewVocabIndex), "ViewVocabIndex");

  // Out-of-range parts are rejected.
  EXPECT_ANY_THROW(Id::makeFromViewVocabIndex(maxViewId + 1, 0, 0));
  EXPECT_ANY_THROW(Id::makeFromViewVocabIndex(0, maxSortHelper + 1, 0));
  EXPECT_ANY_THROW(Id::makeFromViewVocabIndex(0, 0, maxIndex + 1));
}

namespace {
// A mock implementation of `ViewVocabComparisonHooks` for testing the
// `ViewVocabIndex` comparison and hashing logic without a real index. It maps
// `ValueId` bit patterns to explicit string values and buckets, and counts how
// often the (expensive) full string comparison is invoked.
class MockViewVocabHooks : public ViewVocabComparisonHooks {
 public:
  ad_utility::HashMap<uint64_t, std::string> strings_;
  ad_utility::HashMap<uint64_t, uint64_t> buckets_;
  mutable size_t compareByStringCalls_ = 0;

  // Resolve any comparable id to its string: a `LocalVocabIndex` via its entry
  // (like the real hooks would), everything else via the explicit map.
  std::string stringOf(ValueId id) const {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      return id.getLocalVocabIndex()->toStringRepresentation();
    }
    return strings_.at(id.getBits());
  }
  std::string viewVocabString(ValueId id) const override {
    return strings_.at(id.getBits());
  }
  ql::strong_ordering compareByString(ValueId a, ValueId b) const override {
    ++compareByStringCalls_;
    auto sa = stringOf(a);
    auto sb = stringOf(b);
    if (sa < sb) return ql::strong_ordering::less;
    if (sa > sb) return ql::strong_ordering::greater;
    return ql::strong_ordering::equal;
  }
  uint64_t mainVocabBucket(ValueId id) const override {
    return buckets_.at(id.getBits());
  }
};

// RAII helper that registers a hooks instance and clears it on destruction.
struct ScopedViewVocabHooks {
  explicit ScopedViewVocabHooks(const ViewVocabComparisonHooks* hooks) {
    setViewVocabComparisonHooks(hooks);
  }
  ~ScopedViewVocabHooks() { setViewVocabComparisonHooks(nullptr); }
};
}  // namespace

// _____________________________________________________________________________
TEST(ValueId, ViewVocabIndexComparison) {
  MockViewVocabHooks hooks;
  ScopedViewVocabHooks scoped{&hooks};
  auto view = [](uint64_t v, uint64_t sh, uint64_t idx) {
    return Id::makeFromViewVocabIndex(v, sh, idx);
  };

  // Same view: order by the in-vocab index, sort helper is irrelevant, and no
  // string comparison is needed.
  EXPECT_LT(view(1, 5, 10), view(1, 7, 20));
  EXPECT_GT(view(1, 7, 20), view(1, 5, 10));
  EXPECT_EQ(view(1, 5, 10), view(1, 5, 10));
  // Same view, larger sort helper but smaller index still orders by index.
  EXPECT_LT(view(1, 9, 3), view(1, 2, 8));
  EXPECT_EQ(hooks.compareByStringCalls_, 0u);

  // Different views, different sort helper: order by sort helper (bucket), no
  // string comparison.
  EXPECT_LT(view(1, 3, 100), view(2, 8, 0));
  EXPECT_GT(view(2, 8, 0), view(1, 3, 100));
  EXPECT_EQ(hooks.compareByStringCalls_, 0u);

  // Different views, same sort helper: fall back to full string comparison.
  auto apple = view(1, 5, 0);
  auto banana = view(2, 5, 0);
  hooks.strings_[apple.getBits()] = "apple";
  hooks.strings_[banana.getBits()] = "banana";
  EXPECT_LT(apple, banana);
  EXPECT_GT(banana, apple);
  EXPECT_EQ(hooks.compareByStringCalls_, 2u);
}

// _____________________________________________________________________________
TEST(ValueId, ViewVocabIndexComparisonAgainstOtherTypes) {
  MockViewVocabHooks hooks;
  ScopedViewVocabHooks scoped{&hooks};

  // Against a `VocabIndex`: shortcut via the bucket when they differ.
  auto v = Id::makeFromViewVocabIndex(1, 4, 0);
  auto vocab = Id::makeFromVocabIndex(VocabIndex::make(50));
  hooks.buckets_[vocab.getBits()] = 9;  // view bucket 4 < 9
  EXPECT_LT(v, vocab);
  EXPECT_GT(vocab, v);  // inverted correctly
  EXPECT_EQ(hooks.compareByStringCalls_, 0u);

  // Same bucket: fall back to full string comparison.
  auto v2 = Id::makeFromViewVocabIndex(1, 7, 0);
  auto vocab2 = Id::makeFromVocabIndex(VocabIndex::make(60));
  hooks.buckets_[vocab2.getBits()] = 7;
  hooks.strings_[v2.getBits()] = "mango";
  hooks.strings_[vocab2.getBits()] = "lemon";
  EXPECT_GT(v2, vocab2);  // "mango" > "lemon"
  EXPECT_LT(vocab2, v2);  // inverted
  EXPECT_EQ(hooks.compareByStringCalls_, 2u);

  // Against an `EncodedVal`: a `ViewVocabIndex` (a main-vocabulary-space word)
  // is always less, with no string comparison.
  auto encoded = Id::makeFromEncodedVal(1234);
  EXPECT_LT(v, encoded);
  EXPECT_GT(encoded, v);
  EXPECT_EQ(hooks.compareByStringCalls_, 2u);  // unchanged from above

  // Against non-string types: a `ViewVocabIndex` must sort like the other
  // string types, i.e. after numeric/boolean types and before the rest -
  // regardless of its own (highest) datatype value. This is what prevents an
  // ordering cycle such as `view < vocab < geoPoint < view`.
  EXPECT_GT(v, Id::makeFromInt(42));
  EXPECT_GT(v, Id::makeFromDouble(1.5));
  EXPECT_GT(v, Id::makeFromBool(true));
  EXPECT_GT(v, Id::makeUndefined());
  EXPECT_LT(v, Id::makeFromGeoPoint(GeoPoint{0, 0}));
  EXPECT_LT(v, Id::makeFromTextRecordIndex(TextRecordIndex::make(0)));
  EXPECT_LT(v, Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0)));
  // A `VocabIndex` (an actual main-vocabulary string) sorts the same way, so
  // `ViewVocabIndex` is consistent with it.
  auto vocabWord = Id::makeFromVocabIndex(VocabIndex::make(0));
  EXPECT_GT(vocabWord, Id::makeFromInt(42));
  EXPECT_LT(vocabWord, Id::makeFromGeoPoint(GeoPoint{0, 0}));
}

// _____________________________________________________________________________
// Sorting a column that mixes `VocabIndex`, `EncodedVal`, `ViewVocabIndex` and
// non-string types must not violate the strict-weak-ordering requirement (which
// would be undefined behavior in `std::sort`). This exercises the transitivity
// of the comparison across all those combinations.
TEST(ValueId, ViewVocabIndexSortIsConsistent) {
  MockViewVocabHooks hooks;
  ScopedViewVocabHooks scoped{&hooks};

  // A view word "m" in bucket 5, a vocab word bucketed at 3 ("a") and 8 ("z"),
  // an encoded value and a few non-string values.
  auto view = Id::makeFromViewVocabIndex(1, 5, 0);
  auto vocabLow = Id::makeFromVocabIndex(VocabIndex::make(10));
  auto vocabHigh = Id::makeFromVocabIndex(VocabIndex::make(20));
  hooks.buckets_[vocabLow.getBits()] = 3;
  hooks.buckets_[vocabHigh.getBits()] = 8;
  std::vector<ValueId> ids{view,
                           vocabLow,
                           vocabHigh,
                           Id::makeFromEncodedVal(7),
                           Id::makeFromInt(1),
                           Id::makeFromDouble(2.0),
                           Id::makeFromGeoPoint(GeoPoint{1, 1}),
                           Id::makeUndefined()};
  // `std::sort` requires a strict weak ordering; a violation is UB and
  // typically manifests as a crash or a non-sorted result. Sort and verify the
  // invariant
  // `!(b < a)` for every adjacent pair, and that comparison is antisymmetric.
  std::sort(ids.begin(), ids.end());
  for (size_t i = 1; i < ids.size(); ++i) {
    EXPECT_FALSE(ids[i] < ids[i - 1]);
  }
  // Transitivity spot check across the tricky trio.
  EXPECT_LT(view, Id::makeFromEncodedVal(7));  // view < encoded
  EXPECT_LT(view, vocabHigh);                  // bucket 5 < 8
  EXPECT_GT(view, vocabLow);                   // bucket 5 > 3
}

// _____________________________________________________________________________
TEST(ValueId, ViewVocabIndexHashing) {
  MockViewVocabHooks hooks;
  ScopedViewVocabHooks scoped{&hooks};

  // Equal words from different views compare equal and must hash equally.
  auto a = Id::makeFromViewVocabIndex(1, 5, 0);
  auto b = Id::makeFromViewVocabIndex(2, 5, 3);
  hooks.strings_[a.getBits()] = "same";
  hooks.strings_[b.getBits()] = "same";
  EXPECT_EQ(a, b);
  EXPECT_EQ(absl::HashOf(a), absl::HashOf(b));

  // Different words hash (almost surely) differently and compare unequal.
  auto c = Id::makeFromViewVocabIndex(2, 5, 4);
  hooks.strings_[c.getBits()] = "different";
  EXPECT_NE(a, c);
  EXPECT_NE(absl::HashOf(a), absl::HashOf(c));
}

// _____________________________________________________________________________
// A `ViewVocabIndex` and a `LocalVocabIndex` that hold the same string (a word
// not in the main vocabulary) must compare equal AND hash equal, otherwise
// hash-based DISTINCT/GROUP BY/JOIN over such a column would give wrong
// results.
TEST_F(ValueIdTest, ViewVocabIndexEqualsLocalVocab) {
  MockViewVocabHooks hooks;
  ScopedViewVocabHooks scoped{&hooks};

  // A literal that is (almost surely) not in the test index' vocabulary, so the
  // local-vocab entry hashes by its string representation.
  LocalVocabEntry entry = LocalVocabEntry::fromStringRepresentation(
      "\"__view_vocab_absent_literal__\"", qec_->getLocalVocabContext());
  auto localId = ValueId::makeFromLocalVocabIndex(&entry);
  ASSERT_EQ(localId.getDatatype(), Datatype::LocalVocabIndex);

  // A `ViewVocabIndex` whose (mocked) string value equals the local word.
  auto viewId = Id::makeFromViewVocabIndex(3, 6, 42);
  hooks.strings_[viewId.getBits()] = entry.toStringRepresentation();

  EXPECT_EQ(viewId, localId);
  EXPECT_EQ(localId, viewId);  // comparison is symmetric
  EXPECT_EQ(absl::HashOf(viewId), absl::HashOf(localId));

  // A different local word neither compares nor hashes equal.
  LocalVocabEntry other = LocalVocabEntry::fromStringRepresentation(
      "\"__view_vocab_absent_literal_2__\"", qec_->getLocalVocabContext());
  auto otherLocalId = ValueId::makeFromLocalVocabIndex(&other);
  EXPECT_NE(viewId, otherLocalId);
  EXPECT_NE(absl::HashOf(viewId), absl::HashOf(otherLocalId));
}
