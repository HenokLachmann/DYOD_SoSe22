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

  for (const auto& chunk : chunks) {
    resolve_data_type(_column_types.back(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
      chunk->add_segment(value_segment);
    });
  }
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (chunks.back()->size() >= target_chunk_size()) {
    create_new_chunk();
  }

  chunks.back()->append(values);
}

void Table::create_new_chunk() {
  chunks.push_back(std::make_shared<Chunk>());

  for (auto index = 0; index < column_count(); index++) {
    resolve_data_type(_column_types[index], [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>();
      chunks.back()->add_segment(value_segment);
    });
  }
}

ColumnCount Table::column_count() const { return static_cast<ColumnCount>(_column_names.size()); }

ChunkOffset Table::row_count() const {
  auto row_number = 0;

  for (const auto& chunk : chunks) {
    row_number += chunk->size();
  }

  return row_number;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (auto index = 0; index < column_count(); index++) {
    if (_column_names[index] == column_name) {
      return static_cast<ColumnID>(index);
    }
  }

  // If we reach here, the requested column name is non-existent.
  throw std::invalid_argument{"The requested column name does not exist inside the table!"};
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const {
  DebugAssert(column_id < _column_names.size(),
              "The requested column_id has no corresponding column inside the table!");

  return _column_names[column_id];
}

const std::string& Table::column_type(const ColumnID column_id) const {
  DebugAssert(column_id < _column_types.size(),
              "The requested column_id has no corresponding column inside the table!");

  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < chunks.size(), "The requested chunk_id has no corresponding chunk inside the table!");

  return *chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < chunks.size(), "The requested chunk_id has no corresponding chunk inside the table!");

  return *chunks[chunk_id];
}

void Table::compress_chunk(const ChunkID chunk_id) {
  // Implementation goes here
  Fail("Implementation is missing.");
}

}  // namespace opossum
