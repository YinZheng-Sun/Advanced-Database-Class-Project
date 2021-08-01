//
// Created by 孙印政 on 2021/5/15.
//

#include <iostream>
#include "db_IndexIter.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/iterator.h"
#include "util/random.h"
#include <string>

namespace leveldb {
namespace {
class IndexIterator : public Iterator {
public:
    enum Direction { kForward, kReverse };

    IndexIterator(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
                         uint32_t seed)
            : db_(db),
              user_comparator_(cmp),
              iter_(iter),
              sequence_(s),
              direction_(kForward),
              valid_(false),
              rnd_(seed),
              bytes_until_read_sampling_(RandomCompactionPeriod()) { }

    IndexIterator(const IndexIterator&) = delete;
    IndexIterator& operator=(const IndexIterator&) = delete;

    ~IndexIterator() override { delete iter_; }
    bool Valid() const override { return valid_; }
    Slice key() const override {
        assert(valid_);

        if(direction_ == kForward){
            const char* res = ExtractUserKey(iter_->key()).data() + 6;
            int i = 0; //1 0  Index_00000000_00000001
            for(; i < 7; i++) {
                if(*(res +  i) != '0') {
                    break;
                }
            }
            return Slice(res +i,8-i);
        }
        return saved_key_;
//        return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
    }
    Slice value() const override {
        assert(valid_);
        if(direction_ == kForward){
            const char* res = ExtractUserKey(iter_->key()).data() + 15;     // 去除Index_00000000_
            int i = 0;
            for(; i < 7; i++) {
                if(*(res +  i) != '0') {
                    break;
                }
            }
            return Slice(res +i,8-i);
        }
        return Slice(saved_key_.data() + 15,8);
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
    size_t bytes_until_read_sampling_;

    const std::string Get_value(const Slice& target);
};

inline bool IndexIterator::ParseKey(ParsedInternalKey* ikey) {
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

void IndexIterator::Next() {
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

void IndexIterator::FindNextUserEntry(bool skipping, std::string* skip) {
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
                    } else if(user_comparator_->Compare(ikey.user_key, std::string("Index_")) <= 0) {
                        // 不在 Index columnfamily中
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

void IndexIterator::Prev() {
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

void IndexIterator::FindPrevUserEntry() {
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
    if (value_type == kTypeDeletion || (ParseKey(&ikey) && (user_comparator_->Compare(ikey.user_key,"Index_") <= 0) )) {
        // End
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        direction_ = kForward;
    } else {
        valid_ = true;
    }
}

void IndexIterator::Seek(const Slice& target) {
    direction_ = kForward;
    ClearSavedValue();
    saved_key_.clear();
    std::string target_ ="Index_" + std::string(8-target.size(),'0') + target.ToString() + "_";
    AppendInternalKey(&saved_key_,
                      ParsedInternalKey(target_, sequence_, kValueTypeForSeek));

    iter_->Seek(saved_key_);
    if (iter_->Valid()) {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    } else {
        valid_ = false;
    }
}

void IndexIterator::SeekToFirst() {
    direction_ = kForward;
    ClearSavedValue();
    iter_->SeekToFirst();

    if (iter_->Valid()) {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    } else {
        valid_ = false;
    }
}

void IndexIterator::SeekToLast() {
    direction_ = kReverse;
    ClearSavedValue();
    iter_->SeekToLast();
    FindPrevUserEntry();
}

bool IndexIterator::IsInColumnFamily() {
    ParsedInternalKey ikey;
    ParseKey(&ikey);
    return ikey.user_key.starts_with("Index_");
}

//TODO::
inline const std::string IndexIterator::Get_value(const Slice& target) {
    const char* s = target.data();
    return std::string(s+6,8);
}

}

Iterator* NewIndex_Iterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
    return new IndexIterator(db, user_key_comparator, internal_iter, sequence, seed);
}

}