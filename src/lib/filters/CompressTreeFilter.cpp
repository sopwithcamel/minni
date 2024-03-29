#include "CompressTreeFilter.h"

CompressTreeInserter::CompressTreeInserter(Aggregator* agg,
        const Config &cfg,
        HashUtil::HashFunction hf,
		size_t max_keys) :
    AccumulatorInserter(agg, cfg, max_keys),
    hf_(hf),
    next_buffer(0)
{
	uint64_t num_buffers = aggregator_->getNumBuffers();
	send_ = new MultiBuffer<FilterInfo>(num_buffers, 1);
	evicted_list_ = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);

    Setting& c_fanout = readConfigFile(cfg,
            "minni.internal.cbt.fanout");
    uint32_t fanout = c_fanout;
    Setting& c_buffer_size = readConfigFile(cfg,
            "minni.internal.cbt.buffer_size");
    uint32_t buffer_size = c_buffer_size;
    Setting& c_pao_size = readConfigFile(cfg,
            "minni.internal.cbt.pao_size");
    uint32_t pao_size = c_pao_size;
    cbt_ = new cbt::CompressTree(2, fanout, 1000, buffer_size, pao_size,
                aggregator_->ops());
}

CompressTreeInserter::~CompressTreeInserter()
{
	delete evicted_list_;
	delete send_;

    delete cbt_;
}

void* CompressTreeInserter::operator()(void* recv)
{
	char *key;
	string value;
	size_t ind = 0;
    uint64_t hashv;
    void* ptrToHash;
	PartialAgg* pao;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)recv_list->result;
	uint64_t recv_length = (uint64_t)recv_list->length;
	bool flush_on_complete = recv_list->flush_hash;

	FilterInfo* this_send = (*send_)[next_buffer];
	PartialAgg** this_list = (*evicted_list_)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator_->getNumBuffers();
    tokens_processed++;

    cbt_->bulk_insert(pao_l, recv_length);

    if (recv_list->destroy_pao) {
        const Operations* const op = aggregator_->ops();
        for (uint32_t i=0; i<recv_length; i++) {
            pao = pao_l[i];
            op->destroyPAO(pao);
        }
    }
    
    size_t evict_list_ctr = 0;
	if (flush_on_complete || aggregator_->input_finished && 
                tokens_processed == aggregator_->tot_input_tokens && 
                aggregator_->can_exit) {
        bool remain = cbt_->bulk_read(this_list, evict_list_ctr,
                max_keys_per_token);
        if (!remain) {
            aggregator_->can_exit &= true;
            tokens_processed = 0;
        } else
            aggregator_->can_exit &= false;
	}

    this_send->result = this_list;
    this_send->length = evict_list_ctr;
    this_send->destroy_pao = true;
    return this_send;
}
