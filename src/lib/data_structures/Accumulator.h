#ifndef LIB_ACCUMULATOR_H
#define LIB_ACCUMULATOR_H

#include "PartialAgg.h"

class Accumulator
{
  public:
    Accumulator() {}
    ~Accumulator() {}
    virtual bool insert(void* key, PartialAgg* value) = 0;
    virtual bool nextValue(void*& key, PartialAgg*& value) = 0;
};

#endif
