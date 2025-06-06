// Copyright 2025 The QLever Authors, in particular:
//
// 2025        Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "index/DeltaTriples.h"

// _____________________________________________________________________________
TEST(DeltaTriplesCountTest, toJson) {
  constexpr DeltaTriplesCount count{5, 3};
  const nlohmann::json expected = {
      {"inserted", 5}, {"deleted", 3}, {"total", 8}};
  nlohmann::json actual;
  to_json(actual, count);
  EXPECT_THAT(actual, testing::Eq(expected));
}

// _____________________________________________________________________________
TEST(DeltaTriplesCountTest, subtractOperator) {
  constexpr DeltaTriplesCount count1{10, 5};
  constexpr DeltaTriplesCount count2{3, 2};
  EXPECT_THAT(count1 - count2, testing::Eq(DeltaTriplesCount{7, 3}));
  EXPECT_THAT(count2 - count1, testing::Eq(DeltaTriplesCount{-7, -3}));
}
