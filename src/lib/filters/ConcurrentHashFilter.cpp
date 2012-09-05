#include "ConcurrentHashFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

using namespace tbb;

ConcurrentHashInserter::ConcurrentHashInserter(Aggregator* agg,
        const Config &cfg,
		size_t max_keys) :
    AccumulatorInserter(agg, cfg, max_keys),
    next_buffer(0),
    num_evicted(0),
    to_be_evicted(0),
    to_be_cleared_(false)
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

    if (to_be_cleared_)
        ht_->clear();
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

    if (to_be_cleared_) {
        fprintf(stderr, "Clearing hash\n");
        ht_->clear();
        to_be_cleared_ = false;
    }

	// Insert PAOs
    if (recv_length > 0) {
        parallel_for(tbb::blocked_range<PartialAgg**>(pao_l,
                pao_l+recv_length, recv_length/4),
                Aggregate(ht_, recv_list->destroy_pao,
                aggregator_->ops()));
    }
    
	if (flush_on_complete || aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens) {
        if (num_evicted == 0) {
            evict_it = ht_->begin();
            to_be_evicted = ht_->size();
        }
        for (; num_evicted + evict_list_ctr < to_be_evicted; evict_it++) {
            if (aggregator_->ops()->getKey(evict_it->second))
                this_list[evict_list_ctr++] = evict_it->second;
            if (evict_list_ctr == max_keys_per_token)
                break;
        }
        num_evicted += evict_list_ctr;
        fprintf(stderr, "Hash; sent %ld/%ld\n", ht_->size(), num_evicted, to_be_evicted);
        if (evict_list_ctr < max_keys_per_token) {
            aggregator_->can_exit &= true;
            // don't clear right now since that messes with the serializer
            to_be_cleared_ = true;
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
