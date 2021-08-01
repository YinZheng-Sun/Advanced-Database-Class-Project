#include "leveldb/db.h"
#include <cstdio>
#include <iostream>
#include <fstream>
#include <strstream>
#include <sstream>
using namespace std;
using namespace leveldb;
std::string Getval(const char* key);
#include "sys/time.h"

static inline int64_t GetUnixTimeUs() {
    struct timeval tp;
    gettimeofday(&tp, nullptr);
    return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

int main() {
    DB *db = nullptr;
    Options op;
    op.create_if_missing = true;
    Status status = DB::Open(op, "project", &db);

//    ifstream fp("/Users/sunyinzheng/2021课程 /高级数据库/courseforleveldb-master/leveldb/test/data.csv"); //定义声明一个ifstream对象，指定文件路径
//    string line;
//    int count = 0;
//    while (getline(fp,line)){ //循环读取每行数据
//        string key,value;
//        istringstream sin(line); //将整行字符串line读入到字符串流sin中
//
//        getline(sin, key, ','); //将字符串流sin中的字符读入到field字符串中，以逗号为分隔符
//        getline(sin, value, ',');
//        value.erase(value.end() - 1);
//
//        db -> PutWithIndex(WriteOptions(),key,value);
//        if(++count % 2000000 == 0)
//            db->CompactRange(nullptr, nullptr);
//    }
//
    Iterator* iter1 = db->NewIndexIterator(ReadOptions());
//
//    int64_t time1 = GetUnixTimeUs();
    iter1->Seek("600");
    std::cout << iter1->key().ToString();
//    while(iter1->Valid() && iter1->key().compare( "Index_00000701_") <=0 )
//    {
////        std::cout << iter1->key().ToString() << " " <<  iter1->value().ToString() << std::endl;
//        iter1->Next();
//    }
//    int64_t time2 = GetUnixTimeUs();
//    std::cout << "使用索引： " << time2-time1 << std::endl;
//
//    Iterator* iter2 = db->NewIterator(ReadOptions());
//    iter2->SeekToFirst();
//    int64_t time3 = GetUnixTimeUs();
//    assert(iter2->Valid());
//    while(iter2->Valid())
//    {
//        if(iter2->value().compare("00000700")<=0 && iter2->value().compare("00000600") >=0 ) {}
//        iter2->Next();
//    }
//    int64_t time4 = GetUnixTimeUs();
//    std::cout << "未使用索引: " << time4-time3 <<std::endl;
//    std::cout << "" << time5-time1;
    delete iter1;
//    delete iter2;
    delete db;
    return 0;
}
//std::string Getval(const char* key) {
//    return std::string(key+6,8);
//}