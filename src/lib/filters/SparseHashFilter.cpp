#include "SparseHashFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

SparseHashInserter::SparseHashInserter(Aggregator* agg,
        const Config &cfg,
        int num_part,
		size_t max_keys) :
    AccumulatorInserter(agg, cfg, max_keys),
    next_buffer(0),
    numPartitions_(num_part)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);

    Setting& c_capacity = readConfigFile(cfg, "minni.aggregator.bucket.capacity");
    uint64_t capacity = c_capacity;
    sparsehash_ = new SparseHashMurmur(capacity, max_keys_per_token,
            aggregator_->ops());
}

SparseHashInserter::~SparseHashInserter()
{
	delete evicted_list_;
	delete send_;

    delete sparsehash_;
}

int SparseHashInserter::partition(const char* key) const
{
	int buc, sum = 0;
	for (int i=0; i<strlen(key); i++)
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

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;
	bool flush_on_complete = recv_list->flush_hash;
    tokens_processed++;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    size_t evict_list_ctr = 0;

    const Operations* const op = aggregator_->ops();
	// Insert PAOs
    while (ind < recv_length) {
        size_t numEvicted = 0;
        pao = pao_l[ind];
        PartialAgg** l = this_list + evict_list_ctr;
        int p = 0; // partition(op->getKey(pao)); // only internal hashing for now
        if (p == 0) {
            bool ret = sparsehash_->insert((void*)op->getKey(pao), pao, l, numEvicted,
                    max_keys_per_token - evict_list_ctr);
            if (recv_list->destroy_pao && !ret)
                op->destroyPAO(pao);
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
            remain = sparsehash_->nextValue(ptrToHash, this_list[evict_list_ctr]);
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
