#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} { create_new_chunk(); }

void Table::add_column(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);

  for (const auto& chunk : _chunks) {
    resolve_data_type(_column_types.back(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
      chunk->add_segment(value_segment);
    });
  }
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() >= target_chunk_size()) {
    create_new_chunk();
  }

  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  _chunks.push_back(std::make_shared<Chunk>());

  const auto num_cols = column_count();
  for (auto index = 0; index < num_cols; index++) {
    resolve_data_type(_column_types[index], [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
      _chunks.back()->add_segment(value_segment);
    });
  }
}

ColumnCount Table::column_count() const { return static_cast<ColumnCount>(_column_names.size()); }

ChunkOffset Table::row_count() const {
  auto row_number = ChunkOffset{0};

  for (const auto& chunk : _chunks) {
    row_number += chunk->size();
  }

  return row_number;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto target_it = std::find(begin(_column_names), end(_column_names), column_name);
  
  Assert(target_it != _column_names.end(), "The requested column name does not exist inside the table!");

  return static_cast<ColumnID>(target_it - _column_names.begin());
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const {
  return _column_names.at(column_id);
}

const std::string& Table::column_type(const ColumnID column_id) const {
  return _column_types.at(column_id);
}

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  return _chunks.at(chunk_id);
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  return _chunks.at(chunk_id);
}

}  // namespace opossum
