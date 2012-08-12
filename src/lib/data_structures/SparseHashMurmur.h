#ifndef LIB_SPARSEHASHMURMUR_H
#define LIB_SPARSEHASHMURMUR_H

#include <google/sparse_hash_map>
#include <tr1/unordered_map>
#include <string>

#include "HashUtil.h"
#include "PartialAgg.h"

class SparseHashMurmur
{
    struct Murmur {
        uint32_t operator()(const char* key) const
        {
            return HashUtil::MurmurHash(key, strlen(key), 42);
        }
    };
    struct eqstr {
        bool operator()(const char* s1, const char* s2) const
        {
            return (s1 && s2 && strcmp(s1, s2) == 0);
        }
    };
//    typedef std::tr1::unordered_map<const char*, PartialAgg*, Murmur, 
//            eqstr> Hash;
    typedef google::sparse_hash_map<const char*, PartialAgg*, Murmur, 
            eqstr> Hash;
  public:
    SparseHashMurmur(size_t capacity, size_t evictAtTime);
    ~SparseHashMurmur();
    /* returns true if an insert was performed; returns false if
     * merged with existing PAO */
    bool insert(void* key, PartialAgg* value, PartialAgg**& evicted,
            size_t& num_evicted, size_t max_evictable);
    bool nextValue(void*& key, PartialAgg*& value);
    size_t getNumElements() const;
  private:
    Hash accumulator_;
    size_t capacity_;
    const size_t evictAtTime_;
    size_t numElements_;
    Hash::iterator readIterator_;
};

#endif
