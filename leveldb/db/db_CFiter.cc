//
// Created by 孙印政 on 2021/5/15.
//

#include <iostream>
#include "db_CFiter.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include <string.h>
#include <string>

namespace leveldb {
namespace {
class ColumnFamilyIterator : public Iterator {
public:
    enum Direction { kForward, kReverse };

    ColumnFamilyIterator(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
            uint32_t seed, const ColumnFamilyHandle& cf)
    : db_(db),
            user_comparator_(cmp),
            iter_(iter),
            sequence_(s),
            direction_(kForward),
            valid_(false),
    rnd_(seed),
    columnfamily_(cf),
    bytes_until_read_sampling_(RandomCompactionPeriod()) { }

    ColumnFamilyIterator(const ColumnFamilyIterator&) = delete;
    ColumnFamilyIterator& operator=(const ColumnFamilyIterator&) = delete;

    ~ColumnFamilyIterator() override { delete iter_; }
    bool Valid() const override { return valid_; }
    Slice key() const override {
        assert(valid_);
        if(direction_ == kForward) {
            Slice res = ExtractUserKey(iter_->key());
            res.remove_prefix(columnfamily_.GetPrefixSize());
            return res;
        }

//        std::cout << columnfamily_.GetPrefixSize();
//        return (direction_ == kForward) ? ExtractUserKey(iter_->key()).remove_prefix(4) : saved_key_;
        return saved_key_;
    }
    Slice value() const override {
        assert(valid_);
        return (direction_ == kForward) ? iter_->value() : saved_value_;
    }
    Status status() const override {
        if (status_.ok()) {
            return iter_->status();
        } else {
            return status_;
        }
    }

    void Next() override;
    void Prev() override;
    void Seek(const Slice& target) override;
    void SeekToFirst() override;
    void SeekToLast() override;

private:
    void FindNextUserEntry(bool skipping, std::string* skip);
    void FindPrevUserEntry();
    bool ParseKey(ParsedInternalKey* key);
    bool IsInColumnFamily();
    inline void SaveKey(const Slice& k, std::string* dst) {
        dst->assign(k.data(), k.size());
    }

    inline void ClearSavedValue() {
        if (saved_value_.capacity() > 1048576) {
            std::string empty;
            swap(empty, saved_value_);
        } else {
            saved_value_.clear();
        }
    }

    // Picks the number of bytes that can be read until a compaction is scheduled.
    size_t RandomCompactionPeriod() {
        return rnd_.Uniform(2 * config::kReadBytesPeriod);
    }

    DBImpl* db_;
    const Comparator* const user_comparator_;
    Iterator* const iter_;
    SequenceNumber const sequence_;
    Status status_;
    std::string saved_key_;    // == current key when direction_==kReverse
    std::string saved_value_;  // == current raw value when direction_==kReverse
    Direction direction_;
    bool valid_;
    Random rnd_;
    ColumnFamilyHandle columnfamily_;    // TODO:
    size_t bytes_until_read_sampling_;
};

inline bool ColumnFamilyIterator::ParseKey(ParsedInternalKey* ikey) {
    Slice k = iter_->key();

    size_t bytes_read = k.size() + iter_->value().size();
    while (bytes_until_read_sampling_ < bytes_read) {
        bytes_until_read_sampling_ += RandomCompactionPeriod();
        db_->RecordReadSample(k);
    }
    assert(bytes_until_read_sampling_ >= bytes_read);
    bytes_until_read_sampling_ -= bytes_read;

    if (!ParseInternalKey(k, ikey)) {
        status_ = Status::Corruption("corrupted internal key in DBIter");
        return false;
    } else {
        return true;
    }
}

    void ColumnFamilyIterator::Next() {
        assert(valid_);

        if (direction_ == kReverse) {  // Switch directions?
            direction_ = kForward;
            // iter_ is pointing just before the entries for this->key(),
            // so advance into the range of entries for this->key() and then
            // use the normal skipping code below.
            if (!iter_->Valid()) {
                iter_->SeekToFirst();
            } else {
                iter_->Next();
            }
            if (!iter_->Valid()) {
                valid_ = false;
                saved_key_.clear();
                return;
            }
            // saved_key_ already contains the key to skip past.
        } else {
            // Store in saved_key_ the current key so we skip it below.
            SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

            // iter_ is pointing to current key. We can now safely move to the next to
            // avoid checking current key.
            iter_->Next();
            if (!iter_->Valid()) {
                valid_ = false;
                saved_key_.clear();
                return;
            }
        }

        FindNextUserEntry(true, &saved_key_);
    }

    void ColumnFamilyIterator::FindNextUserEntry(bool skipping, std::string* skip) {
        // Loop until we hit an acceptable entry to yield
        assert(iter_->Valid());
        assert(direction_ == kForward);
        do {
            ParsedInternalKey ikey;
            if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                switch (ikey.type) {
                    case kTypeDeletion:
                        SaveKey(ikey.user_key, skip);
                        skipping = true;
                        break;
                    case kTypeValue:
                        //TODO: 处理边界问题
                        if (skipping &&
                                (user_comparator_->Compare(ikey.user_key, *skip) <= 0))  {
                            // Entry hidden
                        } else if(user_comparator_->Compare(ikey.user_key, columnfamily_.GetPrefix()) <= 0) {
                                //不在当前的columnfamily里面
                        }else {
                            valid_ = IsInColumnFamily();
                            saved_key_.clear();
                            return;
                        }
                        break;
                }
            }
            iter_->Next();
        } while (iter_->Valid());
        saved_key_.clear();
        valid_ = false;
    }

    void ColumnFamilyIterator::Prev() {
        assert(valid_);

        if (direction_ == kForward) {  // Switch directions?
            // iter_ is pointing at the current entry.  Scan backwards until
            // the key changes so we can use the normal reverse scanning code.
            assert(iter_->Valid());  // Otherwise valid_ would have been false
            SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
            while (true) {
                iter_->Prev();
                if (!iter_->Valid()) {
                    valid_ = false;
                    saved_key_.clear();
                    ClearSavedValue();
                    return;
                }
                if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
                    0) {
                    break;
                }
            }
            direction_ = kReverse;
        }

        FindPrevUserEntry();
    }

    void ColumnFamilyIterator::FindPrevUserEntry() {
        assert(direction_ == kReverse);

        ValueType value_type = kTypeDeletion;
        if (iter_->Valid()) {
            do {
                ParsedInternalKey ikey;
                if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                    if ((value_type != kTypeDeletion) &&
                        user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
                        // We encountered a non-deleted value in entries for previous keys,
                        break;
                    }
                    value_type = ikey.type;
                    if (value_type == kTypeDeletion) {
                        saved_key_.clear();
                        ClearSavedValue();
                    } else {
                        Slice raw_value = iter_->value();
                        if (saved_value_.capacity() > raw_value.size() + 1048576) {
                            std::string empty;
                            swap(empty, saved_value_);
                        }
                        SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
                        saved_value_.assign(raw_value.data(), raw_value.size());
                    }
                }
                iter_->Prev();
            } while (iter_->Valid());
        }

        //TODO: 处理边界问题
        ParsedInternalKey ikey;
        if (value_type == kTypeDeletion || (ParseKey(&ikey) && (user_comparator_->Compare(ikey.user_key,columnfamily_.GetPrefix()) <= 0) )) {
            // End
            valid_ = false;
            saved_key_.clear();
            ClearSavedValue();
            direction_ = kForward;
        } else {
            valid_ = true;
        }
    }

    void ColumnFamilyIterator::Seek(const Slice& target) {
        direction_ = kForward;
        ClearSavedValue();
        saved_key_.clear();
        AppendInternalKey(&saved_key_,
                          ParsedInternalKey(columnfamily_.GetPrefix() + target.data() , sequence_, kValueTypeForSeek));
        // TODO:: 修改: columnfamily_.GetPrefix() + target.data() 作为 user_key
        iter_->Seek(saved_key_);
        if (iter_->Valid()) {
            FindNextUserEntry(false, &saved_key_ /* temporary storage */);
        } else {
            valid_ = false;
        }
    }

void ColumnFamilyIterator::SeekToFirst() {
    direction_ = kForward;
    ClearSavedValue();
    iter_->SeekToFirst();

    if (iter_->Valid()) {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    } else {
        valid_ = false;
    }
}

void ColumnFamilyIterator::SeekToLast() {
    direction_ = kReverse;
    ClearSavedValue();
    iter_->SeekToLast();
    FindPrevUserEntry();
}

bool ColumnFamilyIterator::IsInColumnFamily() {
    ParsedInternalKey ikey;
    ParseKey(&ikey);
    return ikey.user_key.starts_with(columnfamily_.GetPrefix());
}

}

Iterator* NewCFIterator(DBImpl* db, const Comparator* user_key_comparator,
                    Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed, const ColumnFamilyHandle &cf) {
    return new ColumnFamilyIterator(db, user_key_comparator, internal_iter, sequence, seed, cf);
}

}