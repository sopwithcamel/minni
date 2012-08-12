#include "AccumulatorFilter.h"

static const size_t BUF_SIZE = 65535;

AccumulatorInserter::AccumulatorInserter(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        void (*destroyPAOFunc)(PartialAgg* p),
        const size_t max_keys) :
    filter(serial_in_order),
    aggregator_(agg),
    accumulator_(acc),
    createPAO_(createPAOFunc),
    destroyPAO(destroyPAOFunc),
    max_keys_per_token(max_keys),
    tokens_processed(0)
{
}
