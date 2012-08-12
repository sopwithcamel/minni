#include "CompressTreeFilter.h"

CompressTreeInserter::CompressTreeInserter(Aggregator* agg,
        Accumulator* acc,
        HashUtil::HashFunction hf,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
		void (*destroyPAOFunc)(PartialAgg* p),
		size_t max_keys) :
    AccumulatorInserter(agg, acc, createPAOFunc, destroyPAOFunc, max_keys),
    hf_(hf),
    next_buffer(0)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
}

CompressTreeInserter::~CompressTreeInserter()
{
	delete evicted_list_;
	delete send_;
}

void* CompressTreeInserter::operator()(void* recv)
{
	char *key;
	string value;
	size_t ind = 0;
    uint64_t hashv;
    void* ptrToHash;
	PartialAgg* pao;
    compresstree::CompressTree* ct = (compresstree::CompressTree*)accumulator_;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;
	bool flush_on_complete = recv_list->flush_hash;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    tokens_processed++;

    size_t evict_list_ctr = 0;

    while (ind < recv_length) {
        pao = pao_l[ind];
        switch (hf_) {
            case HashUtil::MURMUR:
                hashv = HashUtil::MurmurHash(pao->key(), 42);
                break;
            case HashUtil::BOB:
                hashv = HashUtil::BobHash(pao->key(), 42);
                break;
        }

        ptrToHash = (void*)&hashv;
        ct->insert(ptrToHash, pao);
        if (recv_list->destroy_pao)
            destroyPAO(pao);
        ind++;
    }

    assert(evict_list_ctr < max_keys_per_token);
	if (flush_on_complete || aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens && 
                aggregator_->can_exit) {
        uint64_t hash;
        void* ptrToHash = (void*)&hash;
        bool remain;
        while(true) {
            if (evict_list_ctr == max_keys_per_token) {
                aggregator_->stall_pipeline |= true;
                aggregator_->can_exit &= false;
                break;
            }
            remain = ct->nextValue(ptrToHash, this_list[evict_list_ctr]);
            if (!remain) {
                aggregator_->can_exit &= true;
                break;
            }
            evict_list_ctr++;
        }
	}

    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    this_send->destroy_pao = true;
    return this_send;
}
