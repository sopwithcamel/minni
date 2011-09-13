#ifndef LIB_INTERNALHASHER_H
#define LIB_INTERNALHASHER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Mapper.h"
#include "Aggregator.h"
#include "Util.h"
#include "uthash.h"

/**
 * - Consumes: array of PAOs to be aggregated
 * - Produces: array of PAOs evicted from in-memory HT 
 * 
 * TODO:
 * - overload += operator in user-defined PartialAgg class
 * - pass in partitioning function as a parameter
 * - change to TBB hash_map which is parallel access
 */

class Hasher : public tbb::filter {
public:
	Hasher(Aggregator* agg, PartialAgg* emptyPAO, size_t capacity, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~Hasher();
	void (*destroyPAO)(PartialAgg* p);
	void setFlushOnComplete();
private:
	Aggregator* aggregator;
	PartialAgg* emptyPAO;
	PartialAgg*** evicted_list;
	size_t ht_size;
	size_t ht_capacity;
	size_t next_buffer;
	FilterInfo** send;
	PartialAgg* hashtable;
	uint64_t tokens_processed;
	bool flush_on_complete;
	void* operator()(void* pao_list);
};

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
		}
		if (ht_size > ht_capacity) { // PAO has to be evicted
			HASH_DEL(hashtable, pao_l[evict_ind++]);
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
#endif // LIB_INTERNALHASHER_H
