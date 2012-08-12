#include "SparseHashFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

SparseHashInserter::SparseHashInserter(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
		void (*destroyPAOFunc)(PartialAgg* p),
        int num_part,
		size_t max_keys) :
    AccumulatorInserter(agg, acc, createPAOFunc, destroyPAOFunc, max_keys),
    next_buffer(0),
    numPartitions_(num_part)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
}

SparseHashInserter::~SparseHashInserter()
{
	delete evicted_list_;
	delete send_;
}

int SparseHashInserter::partition(const std::string& key)
{
	int buc, sum = 0;
	for (int i=0; i<key.size(); i++)
		sum += key[i];
	buc = (sum) % numPartitions_;
	if (buc < 0)
		buc += numPartitions_;
	return buc;
}

void* SparseHashInserter::operator()(void* recv)
{
	char *key;
	size_t ind = 0;
	PartialAgg* pao;
//    SparseHash* sh = (SparseHash*)accumulator_;
    Accumulator* sh = accumulator_;

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
    while (ind < recv_length) {
        size_t numEvicted = 0;
        pao = pao_l[ind];
        PartialAgg** l = this_list + evict_list_ctr;
        int p = partition(pao->key());
        if (p == 0) {
            bool ret = sh->insert((void*)pao->key().c_str(), pao, l, numEvicted,
                    max_keys_per_token - evict_list_ctr);
            if (recv_list->destroy_pao && !ret)
                destroyPAO(pao);
        } else {
            this_list[evict_list_ctr++] = pao;
        }
        ind++;
    }
    assert(evict_list_ctr <= max_keys_per_token);
    
	if (flush_on_complete || aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens) {
        uint64_t hash;
        void* ptrToHash = (void*)&hash;
        bool remain;
        while(true) {
            if (evict_list_ctr == max_keys_per_token)
                break;
            remain = sh->nextValue(ptrToHash, this_list[evict_list_ctr]);
            if (!remain) {
                aggregator_->can_exit &= true;
                break;
            } else {
                aggregator_->stall_pipeline |= true;
                aggregator_->can_exit &= false;
            }
            evict_list_ctr++;
        }
	} else
        aggregator_->can_exit &= false;
ship_tokens:
    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    this_send->destroy_pao = true;
    return this_send;
}
