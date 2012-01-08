#include "AccumulatorFilter.h"

AccumulatorInserter::AccumulatorInserter(Aggregator* agg,
        Accumulator* acc,
        void (*destroyPAOFunc)(PartialAgg* p),
        const size_t max_keys) :
    filter(/*serial=*/true),
    aggregator_(agg),
    accumulator_(acc),
    destroyPAO(destroyPAOFunc),
    max_keys_per_token(max_keys)
{
}


AccumulatorInserter::~AccumulatorInserter()
{
}


AccumulatorReader::AccumulatorReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const size_t max_keys) :
    filter(/*serial=*/true),
    aggregator_(agg),
    accumulator_(acc),
    createPAO(createPAOFunc),
    max_keys_per_token(max_keys)
{
}
    
AccumulatorReader::~AccumulatorReader()
{
}
