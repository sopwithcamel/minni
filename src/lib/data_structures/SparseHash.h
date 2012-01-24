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
    static uint32_t MurmurHash(const void* buf, size_t len, uint32_t seed)
    {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.

        const unsigned int m = 0x5bd1e995;
        const int r = 24;

        // Initialize the hash to a 'random' value
        uint32_t h = seed ^ len;

        // Mix 4 bytes at a time into the hash
        const unsigned char * data = (const unsigned char *)buf;

        while(len >= 4) {
            unsigned int k = *(unsigned int *)data;

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;

            data += 4;
            len -= 4;
        }

        // Handle the last few bytes of the input array
        switch(len) {
            case 3: h ^= data[2] << 16;
            case 2: h ^= data[1] << 8;
            case 1: h ^= data[0];
                    h *= m;
        };

        // Do a few final mixes of the hash to ensure the last few
        // bytes are well-incorporated.
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;
        return h;
    }
    struct Murmur {
        uint32_t operator()(const char* key) const
        {
            return MurmurHash(key, strlen(key), 42);
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
