#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = StorageManager::get();
    auto table_a = std::make_shared<Table>();
    auto table_b = std::make_shared<Table>(4);

    storage_manager.add_table("first_table", table_a);
    storage_manager.add_table("second_table", table_b);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& storage_manager = StorageManager::get();
  auto table_c = storage_manager.get_table("first_table");
  auto table_d = storage_manager.get_table("second_table");
  EXPECT_THROW(storage_manager.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& storage_manager = StorageManager::get();
  storage_manager.drop_table("first_table");
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
  EXPECT_THROW(storage_manager.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::get().reset();
  auto& storage_manager = StorageManager::get();
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, TableNames) {
  const auto& storage_manager = StorageManager::get();
  const std::vector<std::string> expected_names{"first_table", "second_table"};
  EXPECT_EQ(storage_manager.table_names(), expected_names);
}

TEST_F(StorageStorageManagerTest, PrintTableInfo) {
  const auto& storage_manager = StorageManager::get();

  std::ostringstream oss{};
  storage_manager.print(oss);

  EXPECT_NE(oss.str().find("Name: first_table"), std::string::npos);
  EXPECT_NE(oss.str().find("# Columns: 0"), std::string::npos);
  EXPECT_NE(oss.str().find("# Rows: 0"), std::string::npos);
  // Even empty tables contain one chunk of data.
  EXPECT_NE(oss.str().find("# Chunks: 1"), std::string::npos);
}

}  // namespace opossum
