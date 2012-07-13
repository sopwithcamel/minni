#ifndef LIB_ACCUMULATOR_H
#define LIB_ACCUMULATOR_H

#include "PartialAgg.h"

class Accumulator
{
  public:
    Accumulator() {}
    virtual ~Accumulator() {}
    virtual bool insert(void* key, PartialAgg* value, PartialAgg**& evicted,
            size_t& num_evicted, size_t max_evictable) {}
    virtual bool insert(void* key, PartialAgg* value) {}
    virtual bool nextValue(void*& key, PartialAgg*& value) = 0;
};

#endif
