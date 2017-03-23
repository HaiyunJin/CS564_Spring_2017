/*
 * @author Haiyun Jin
 */

#include "empty_btree_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

EmptyBTreeException::EmptyBTreeException()
    : BadgerDbException("")
{
  std::stringstream ss;
  ss << "This index file is empty.";
  message_.assign(ss.str());
}

}
