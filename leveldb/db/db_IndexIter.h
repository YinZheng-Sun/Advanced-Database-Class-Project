//
// Created by 孙印政 on 2021/5/17.
//

#ifndef LEVELDB_DB_INDEXITER_H
#define LEVELDB_DB_INDEXITER_H

#include <stdint.h>

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

    class DBImpl;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
    Iterator* NewIndex_Iterator(DBImpl* db, const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed);

}  // namespace leveldb

#endif //LEVELDB_DB_INDEXITER_H
