#ifndef LIB_SPARSEHASH_H
#define LIB_SPARSEHASH_H

#include <google/sparse_hash_map>
#include <tr1/unordered_map>
#include <string>

#include "Accumulator.h"
#include "PartialAgg.h"

class SparseHash :
        public Accumulator
{
//    typedef std::tr1::unordered_map<std::string, PartialAgg*> Hash;
    typedef google::sparse_hash_map<std::string, PartialAgg*> Hash;
  public:
    SparseHash(size_t capacity);
    ~SparseHash();
    /* returns true if an insert was performed; returns false if
     * merged with existing PAO */
    bool insert(void* key, PartialAgg* value, PartialAgg**& evicted,
            size_t& num_evicted);
    bool nextValue(void*& key, PartialAgg*& value);
  private:
    Hash accumulator_;
    size_t capacity_;
    const size_t evictAtTime_;
    size_t numElements_;
    Hash::iterator readIterator_;
};

#endif
