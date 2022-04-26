#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) { _segments.push_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(),
              "The number of values doesn't match the number of columns in the chunk!");

  for (auto index = 0; index < column_count(); index++) {
    _segments[index]->append(values[index]);
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const { return _segments[column_id]; }

ColumnCount Chunk::column_count() const { return static_cast<ColumnCount>(_segments.size()); }

ChunkOffset Chunk::size() const {
  if (_segments.empty()) {
    return 0;
  } else {
    return _segments[0]->size();
  }
}

}  // namespace opossum
