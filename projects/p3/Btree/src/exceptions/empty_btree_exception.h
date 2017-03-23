/*
 * @author Haiyun Jin
 */

#pragma once

#include "badgerdb_exception.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when try to delete entry from an empty btree index file
 */
class EmptyBTreeException : public BadgerDbException {
 public:
  /**
   * Constructs a empty btree exception
   */
  explicit EmptyBTreeException();
};

}
