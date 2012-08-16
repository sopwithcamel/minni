#include "ConcurrentHashFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

using namespace tbb;

ConcurrentHashInserter::ConcurrentHashInserter(Aggregator* agg,
        const Config &cfg,
		size_t max_keys) :
    AccumulatorInserter(agg, cfg, max_keys),
    next_buffer(0),
    num_evicted(0)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
    ht_ = new Hashtable();
}

ConcurrentHashInserter::~ConcurrentHashInserter()
{
	delete evicted_list_;
	delete send_;

    delete ht_;
}

void* ConcurrentHashInserter::operator()(void* recv)
{
	char *key;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;
	bool flush_on_complete = recv_list->flush_hash;
    tokens_processed++;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    size_t evict_list_ctr = 0;

	// Insert PAOs
    if (recv_length > 0) {
        parallel_for(tbb::blocked_range<PartialAgg**>(pao_l,
                pao_l+recv_length, 100),
                Aggregate(ht_, recv_list->destroy_pao,
                aggregator_->ops()));
    }
    
	if (flush_on_complete || aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens) {
        if (num_evicted == 0)
            evict_it = ht_->begin();
        for (; evict_it != ht_->end(); ++evict_it) {
            this_list[evict_list_ctr++] = evict_it->second;
            if (evict_list_ctr == max_keys_per_token)
                break;
        }
        fprintf(stderr, "Hash has %ld elements; sending %ld\n", ht_->size(), evict_list_ctr);
        num_evicted += evict_list_ctr;
        if (evict_list_ctr < max_keys_per_token) {
            aggregator_->can_exit &= true;
            ht_->clear();
            num_evicted = 0;
        } else {
            aggregator_->stall_pipeline |= true;
            aggregator_->can_exit &= false;
        }
	} else {
        aggregator_->can_exit &= false;
    }
ship_tokens:
    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    this_send->destroy_pao = true;
    return this_send;
}
