#ifndef LIB_SPARSEHASH_H
#define LIB_SPARSEHASH_H

#include <google/sparse_hash_map>

#include "Accumulator.h"
#include "PartialAgg.h"

class SparseHash :
        public Accumulator
{
    typedef google::sparse_hash_map<int64_t, PartialAgg*> Hash;
  public:
    SparseHash(size_t capacity);
    ~SparseHash();
    bool insert(void* key, PartialAgg* value, PartialAgg**& evicted,
            size_t& num_evicted);
    bool nextValue(void*& key, PartialAgg*& value);
  private:
    Hash accumulator_;
    size_t capacity_;
    const size_t evictAtTime_;
    size_t numElements_;
};

#endif
