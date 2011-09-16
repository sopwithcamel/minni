#include "Hasher.h"

#define DEL_THRESHOLD		1000

Hasher::Hasher(Aggregator* agg, 
			PartialAgg* emptyPAO,
			size_t capacity,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		ht_size(0),
		ht_capacity(capacity),
		hashtable(NULL),
		next_buffer(0),
		flush_on_complete(false),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	evicted_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers);
	send = (FilterInfo**)malloc(sizeof(FilterInfo*) * num_buffers);
	// Allocate buffers and structure to send results to next filter
	for (int i=0; i<num_buffers; i++) {
		evicted_list[i] = (PartialAgg**)malloc(sizeof(PartialAgg*) * MAX_KEYS_PER_TOKEN);
		send[i] = (FilterInfo*)malloc(sizeof(FilterInfo));
	}
}

Hasher::~Hasher()
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	PartialAgg *s, *tmp;
	HASH_ITER(hh, hashtable, s, tmp) {
		HASH_DEL(hashtable, s);
		free(s);
	}
	for (int i=0; i<num_buffers; i++) {
		free(evicted_list[i]);
		free(send[i]);
	}
	free(evicted_list);
	free(send);
}

void* Hasher::operator()(void* pao_list)
{
	char *key, *value;
	uint64_t ind = 0;
	uint64_t evict_ind = 0;
	PartialAgg* pao;
	size_t evict_list_ctr = 0;

	FilterInfo* recv = (FilterInfo*)pao_list;
	PartialAgg** pao_l = (PartialAgg**)recv->result;
	uint64_t recv_length = (uint64_t)recv->length;	

	PartialAgg** this_list = evicted_list[next_buffer];
	FilterInfo* this_send = send[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

	// PAO to be evicted next. We maintain pointer because begin() on an unordered
	// map is an expensive operation!
	PartialAgg* next_evict;

	while (ind < recv_length) {
		pao = pao_l[ind];
		PartialAgg* found = NULL;
		HASH_FIND_STR(hashtable, pao->key, found);
		if (found) { // the insertion didn't occur
			found->merge(pao);
			destroyPAO(pao);
		} else { // the PAO was inserted
			HASH_ADD_KEYPTR(hh, hashtable, pao->key, strlen(pao->key), pao);
			ht_size++;
		}
		if (ht_size > ht_capacity + DEL_THRESHOLD) { // PAO has to be evicted
			PartialAgg *entry, *tmp;
			HASH_ITER(hh, hashtable, entry, tmp) {
				this_list[evict_list_ctr++] = entry;
				assert(evict_list_ctr < MAX_KEYS_PER_TOKEN);
				HASH_DEL(hashtable, entry);
			}
		}
		ind++;
	}

	/* next bit of code tests whether the input stage has completed. If
	   it has, and the number of tokens processed by the input stage is
	   equal to the number processed by this one, then it also adds all
	   the PAOs in the hash table to the list and clears the hashtable
           essentially flushing all global state. */

	tokens_processed++;
	// if the number of buffers > 1 then some might be queued up
	if (flush_on_complete || (aggregator->input_finished && 
			tokens_processed == aggregator->tot_input_tokens)) {
		PartialAgg *s, *tmp;
		HASH_ITER(hh, hashtable, s, tmp) {
			this_list[evict_list_ctr++] = s;
			assert(evict_list_ctr < MAX_KEYS_PER_TOKEN);
		}	
		HASH_CLEAR(hh, hashtable);
	}
	this_send->result = this_list;
	this_send->length = evict_list_ctr;
	return this_send;
}

void Hasher::setFlushOnComplete()
{
	flush_on_complete = true;
}
