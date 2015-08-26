#include "gtest/gtest.h"

TEST(storage, True)
{
  EXPECT_EQ(1, 1);
}

TEST(storage, False)
{
  EXPECT_NE(1, 2);
}
