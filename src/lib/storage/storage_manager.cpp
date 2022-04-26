#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager manager{};
  return manager;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) { _tables[name] = table; }

void StorageManager::drop_table(const std::string& name) {
  if (!has_table(name)) {
    throw std::invalid_argument{"The table does not exist inside the storage manager!"};
  }

  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  // We cannot use operator[] here because that triggers a side effect on key miss.
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names{};

  for (const auto& table : _tables) {
    table_names.push_back(table.first);
  }

  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  out << "Table Information:" << std::endl;

  for (const auto& table : _tables) {
    out << "Name: " << table.first;
    out << "\n# Columns: " << table.second->column_count();
    out << "\n# Rows: " << table.second->row_count();
    out << "\n# Chunks: " << table.second->chunk_count();
    out << "\n" << std::endl;
  }
}

void StorageManager::reset() { _tables.clear(); }

}  // namespace opossum
