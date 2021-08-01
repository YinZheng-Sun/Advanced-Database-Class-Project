//
// Created by 孙印政 on 2021/5/15.
//

#ifndef LEVELDB_DB_CFITER_H
#define LEVELDB_DB_CFITER_H

#include <stdint.h>

#include "db/dbformat.h"
#include "leveldb/db.h"
namespace leveldb {

class DBImpl;

Iterator* NewCFIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed, const ColumnFamilyHandle &cf);
}


#endif //LEVELDB_DB_CFITER_H
