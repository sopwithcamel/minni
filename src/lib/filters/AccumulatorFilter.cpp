#include "AccumulatorFilter.h"

static const size_t BUF_SIZE = 65535;

AccumulatorInserter::AccumulatorInserter(Aggregator* agg,
        const Config &cfg,
        const size_t max_keys) :
    filter(serial_in_order),
    aggregator_(agg),
    cfg_(cfg),
    max_keys_per_token(max_keys),
    tokens_processed(0)
{
}

void AccumulatorInserter::reset()
{
    tokens_processed = 0;
}
