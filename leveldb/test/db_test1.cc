#include "leveldb/db.h"
#include <cstdio>
#include <iostream>

using namespace std;
using namespace leveldb;

int main() {
    DB* db = nullptr;
    std::string value;
    Options op;
    op.create_if_missing = true;
    op.with_hashmap = true;
    op.with_hashmap = true;
    Status status = DB::Open(op, "TEST_PUT__", &db);

    db->Put(WriteOptions(),"cf1","1","01");
    db->Put(WriteOptions(),"0000000","01");
    db->Put(WriteOptions(),"0000001","01");
    db->Put(WriteOptions(),"00000001","01");
    db->Put(WriteOptions(),"0000002","01");
//    Iterator* iter1 = db->NewIterator(ReadOptions());
    Iterator* iter3 = db->NewColumnFamilyIterator(ReadOptions(),"cf1");
//    iter1->SeekToFirst();
//    std::cout << iter1->key().ToString() + " : "<<iter1->value().ToString() << std::endl;
//    iter1->Next();
//    std::cout << iter1->key().ToString()+ " : "<<iter1->value().ToString() << std::endl;
//
    iter3->SeekToFirst();
    assert(iter3->Valid());
    std::cout << iter3->key().ToString() << std::endl;
    std::cout << iter3->key().ToString();
//    while(iter1->Valid()){
//        std::cout << iter1->key().ToString() << std::endl;
//        iter1->Next();
//    }
//    iter1->Seek("00000002");
//    std::cout << iter1->key().ToString() << std::endl;

//    Iterator* iter2 = db->NewIndexIterator(ReadOptions());
//    iter2->SeekToFirst();
//    iter2->Seek("0");
//    delete iter1;
//    delete iter1;
    delete iter3;
    delete db;
    return 0;
}

