#include "SparseHashFilter.h"

#define Hash    HASH_FCN
#define HASH_FUNCTION   HASH_MUR

SparseHashInserter::SparseHashInserter(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
		void (*destroyPAOFunc)(PartialAgg* p),
		size_t max_keys) :
    AccumulatorInserter(agg, acc, destroyPAOFunc, max_keys),
    createPAO_(createPAOFunc),
    next_buffer(0)
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

void* SparseHashInserter::operator()(void* recv)
{
	char *key;
	size_t ind = 0;
	PartialAgg* pao;
    SparseHash* sh = (SparseHash*)accumulator_;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;
	bool flush_on_complete = recv_list->flush_hash;
    tokens_processed++;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    size_t numEvicted = 0;
    size_t evict_list_ctr = 0;

	// Insert PAOs
    while (ind < recv_length) {
        pao = pao_l[ind];
        PartialAgg** l = this_list + evict_list_ctr;
        bool ret = sh->insert((void*)pao->key.c_str(), pao, l, numEvicted);
        evict_list_ctr += numEvicted;
        if (recv_list->destroy_pao && !ret)
            destroyPAO(pao);
        ind++;
    }
    assert(evict_list_ctr <= max_keys_per_token);
    
	if (flush_on_complete || aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens) {
        uint64_t hash;
        void* ptrToHash = (void*)&hash;
        bool remain;
        if (evict_list_ctr == max_keys_per_token) {
            aggregator_->sendNextToken = false; // i'm not done yet!
            goto ship_tokens;
        }
        while(sh->nextValue(ptrToHash, this_list[evict_list_ctr])) {
//            fprintf(stderr, "read: key: %s\n", this_list[evict_list_ctr]->key);
            evict_list_ctr++;
            if (evict_list_ctr == max_keys_per_token) {
                aggregator_->sendNextToken = false; // i'm not done yet!
                break;
            }
        }
	}
ship_tokens:
    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    this_send->destroy_pao = true;
    return this_send;
}

SparseHashReader::SparseHashReader(Aggregator* agg, 
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const size_t max_keys) :
    AccumulatorReader(agg, acc, createPAOFunc, max_keys)
{
}

SparseHashReader::SparseHashReader(Aggregator* agg,
        Accumulator* acc,
        size_t (*createPAOFunc)(Token* t, PartialAgg** p),
        const char* outfile_prefix) :
    AccumulatorReader(agg, acc, createPAOFunc, outfile_prefix)
{
}

SparseHashReader::~SparseHashReader()
{
}

void* SparseHashReader::operator()(void* recv)
{
    if (writeToFile_) {
        PartialAgg* pao;
        uint64_t buc;
        bool valid, remove;
        string val;
        SparseHash* bt = (SparseHash*)accumulator_;
        uint64_t n_part = aggregator_->getNumPartitions();
        uint64_t hash;
        createPAO_(NULL, &pao);
        void* ptrToHash = (void*)&hash;
        while (bt->nextValue(ptrToHash, pao)) {
            buc = *(uint64_t*)ptrToHash % n_part;
            assert(buc >= 0);
            pao->serialize(buf_);
            fwrite(buf_, 1, strlen(buf_), fl_[buc]);
        }
        fprintf(stderr, "Closing files\n");
        for (int i=0; i<n_part; i++)
            fclose(fl_[i]);
        return NULL;
    } else {
        fprintf(stderr, "Not implemented yet!");
        assert(false);
    }
}
