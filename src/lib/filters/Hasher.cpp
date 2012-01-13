#include "Hasher.h"
#include "Hashtable.h"

Hasher::Hasher(Aggregator* agg, Hashtable* ht,
			void (*destroyPAOFunc)(PartialAgg* p),
			const size_t max_keys) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
        hashtable(ht),
		destroyPAO(destroyPAOFunc),
		max_keys_per_token(max_keys),
		next_buffer(0),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	evicted_list = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
	merge_list = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
	mergand_list = new MultiBuffer<PartialAgg*>(num_buffers,
			max_keys_per_token);
	send = new MultiBuffer<FilterInfo>(num_buffers, 1);
}

Hasher::~Hasher()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	delete evicted_list;
	delete merge_list;
	delete mergand_list;
	delete send;
}

void* Hasher::operator()(void* recv)
{
	char *key, *value;
	uint64_t ind = 0;
	uint64_t evict_ind = 0;
	PartialAgg* pao;
	size_t evict_list_ctr = 0;
	size_t merge_list_ctr = 0;

	FilterInfo* recv_list = (FilterInfo*)recv;
	PartialAgg** pao_l = (PartialAgg**)(recv_list->result);
	uint64_t recv_length = (uint64_t)(recv_list->length);	
	bool flush_on_complete = recv_list->flush_hash;

	PartialAgg** this_list = (*evicted_list)[next_buffer];
	PartialAgg** this_merge_list = (*merge_list)[next_buffer];
	PartialAgg** this_mergand_list = (*mergand_list)[next_buffer];
	FilterInfo* this_send = (*send)[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

    if (recv_length > 0) {
        while (ind < recv_length) {
            pao = pao_l[ind];
            PartialAgg* found = NULL;
            hashtable->search(pao->key, found);
            if (found) { // the insertion didn't occur
                this_merge_list[merge_list_ctr] = found;
                this_mergand_list[merge_list_ctr++] = pao;
            } else { // the PAO was inserted
                size_t num_evicted = hashtable->insert(pao->key,
                        strlen(pao->key), pao, this_list + evict_list_ctr);
                evict_list_ctr += num_evicted;
                assert(evict_list_ctr < max_keys_per_token);
            }
            ind++;
        }
        tokens_processed++;
    }

	/* next bit of code tests whether the input stage has completed. If
	   it has, and the number of tokens processed by the input stage is
	   equal to the number processed by this one, then it also adds all
	   the PAOs in the hash table to the list and clears the hashtable
           essentially flushing all global state. */
	// if the number of buffers > 1 then some might be queued up
	if (flush_on_complete || (aggregator->input_finished && 
                tokens_processed == aggregator->tot_input_tokens)) {
        size_t num_evicted;
        bool evictDone = hashtable->evictAll(this_list + evict_list_ctr, 
                num_evicted, max_keys_per_token - evict_list_ctr);
        if (!evictDone) {
            aggregator->sendNextToken = false; // i'm not done yet!
        }
        evict_list_ctr += num_evicted;
	}
	this_send->result = this_list;
	this_send->length = evict_list_ctr;
	this_send->result1 = this_merge_list;
	this_send->result2 = this_mergand_list;
	this_send->result3 = merge_list_ctr;
	return this_send;
}
