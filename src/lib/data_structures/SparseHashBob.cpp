#include "SparseHashBob.h"
#include <algorithm>

SparseHashBob::SparseHashBob(size_t capacity, size_t evictAtTime) :
        capacity_(capacity),
        evictAtTime_(evictAtTime),
        numElements_(0)
{
    accumulator_.set_deleted_key("iapatanipalhimam");
}

SparseHashBob::~SparseHashBob()
{
}

bool SparseHashBob::insert(void* key, PartialAgg* value, PartialAgg**& evicted,
        size_t& num_evicted, size_t max_evictable)
{
    bool ret;
    char* k = (char*)key;
    num_evicted = 0;
    PartialAgg* mg = NULL;
    mg = accumulator_[k];
    if (mg) {
        mg->merge(value);
        ret = false;
    } else {
        ret = true;
        accumulator_[k] = value;
        numElements_++;
        // insert invalidates read iterator
        readIterator_ = accumulator_.end();
    }
    return ret;
}

bool SparseHashBob::nextValue(void*& key, PartialAgg*& value)
{
    if (readIterator_ == accumulator_.end())
        readIterator_ = accumulator_.begin();
    if (readIterator_ != accumulator_.end()) {
        key = (void*)(readIterator_->first);
        value = readIterator_->second;
        //  erase keeps iterator valid
        Hash::iterator t = readIterator_++;
        accumulator_.erase(t);
        numElements_--;
        return true;
    }
    accumulator_.clear();
    numElements_ = 0;
    return false;
}

size_t SparseHashBob::getNumElements() const
{
    return numElements_;
}
