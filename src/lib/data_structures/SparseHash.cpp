#include "SparseHash.h"

SparseHash::SparseHash(size_t capacity) :
        capacity_(capacity),
        evictAtTime_(capacity_ >> 7),
        numElements_(0)
{
//    accumulator_.set_deleted_key("mamihlapinatapai");
}

SparseHash::~SparseHash()
{
}

bool SparseHash::insert(void* key, PartialAgg* value, PartialAgg**& evicted,
        size_t& num_evicted)
{
    std::string k = std::string((char*)key);
    PartialAgg* mg = accumulator_[k];
    if (mg)
        mg->merge(value);
    else
        accumulator_[k] = value;
    if (++numElements_ >= capacity_) {
        size_t evictCtr = 0;
        for (Hash::iterator it=accumulator_.begin(); it != accumulator_.end(); 
                it++) {
            evicted[evictCtr++] = it->second;
            accumulator_.erase(it);
            if (evictCtr == evictAtTime_)
                break;
        }
        numElements_ -= evictCtr;
        num_evicted = evictCtr;
    } else
        num_evicted = 0;
    // insert invalidates read iterator
    readIterator_ = accumulator_.end();
    return true;
}

bool SparseHash::nextValue(void*& key, PartialAgg*& value)
{
    if (readIterator_ == accumulator_.end())
        readIterator_ = accumulator_.begin();
    if (readIterator_ != accumulator_.end()) {
        key = (void*)(readIterator_->first).c_str();
        value = readIterator_->second;
        //  erase keeps iterator valid
        Hash::iterator t = readIterator_++;
        accumulator_.erase(t);
        return true;
    }
    return false;
}
