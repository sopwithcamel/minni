#include "SparseHash.h"

SparseHash::SparseHash(size_t capacity) :
        capacity_(capacity),
        evictAtTime_(capacity_ >> 7),
        numElements_(0)
{
    accumulator_.set_deleted_key(-1);
}

SparseHash::~SparseHash()
{
}

bool SparseHash::insert(void* key, PartialAgg* value, PartialAgg**& evicted,
        size_t& num_evicted)
{
    accumulator_[*(uint64_t*)key] = value;
    if (++numElements_ >= capacity_) {
        size_t evictCtr = 0;
        for (Hash::iterator it=accumulator_.begin(); it != accumulator_.end(); 
                it++) {
            evicted[evictCtr++] = it->second;
            accumulator_.erase(it);
        }
        num_evicted = evictCtr;
    }
}

bool SparseHash::nextValue(void*& key, PartialAgg*& value)
{
    Hash::iterator it = accumulator_.begin();
    key = (void*)it->first;
    value = it->second;
    accumulator_.erase(it);
}
