#ifndef LIB_EXTERNALHASHER_H
#define LIB_EXTERNALHASHER_H

#include <stdlib.h>
#include <iostream>
#include <tr1/unordered_map>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "PartialAgg.h"
#include "Mapper.h"
#include "MapperAggregator.h"
#include "Util.h"

#define HT_CAPACITY		500000

/**
 * - Consumes: array of PAOs to be aggregated into an external hashtable
 * - Produces: nil
 * 
 * TODO:
 * - overload += operator in user-defined PartialAgg class
 * - pass in partitioning function as a parameter
 * - change to TBB hash_map which is parallel access
 */

class ExternalHasher : public tbb::filter {
public:
	ExternalHasher(MapperAggregator* agg,
			const char* ht_name,
			PartialAgg* emptyPAO, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~ExternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	MapperAggregator* aggregator;
	FawnDS<FawnDS_Flash> *evictHash;
	size_t ht_size;
	uint64_t tokens_processed;
	void* operator()(void* pao_list);
};

ExternalHasher::ExternalHasher(MapperAggregator* agg, 
			const char* ht_name,
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		ht_size(0),
		tokens_processed(0)
{
}

ExternalHasher::~ExternalHasher()
{
}

void* ExternalHasher::operator()(void* pao_list)
{
	char *key, *value;
	PartialAgg** pao_l = (PartialAgg**)pao_list;
	std::pair<typename Hash::iterator, bool> result;
	size_t ind = 0;
	size_t last_flush_ind = 0;
	PartialAgg* pao;

	size_t evict_list_ctr = 0;
	size_t evict_list_size = 1;
	evicted_list = (PartialAgg**)malloc(sizeof(PartialAgg*));

	// PAO to be evicted next. We maintain pointer because begin() on an unordered
	// map is an expensive operation!
	Hash::iterator next_evict = hashtable.end();

	while (strcmp((pao_l[ind])->key, EMPTY_KEY)) { 
		pao = pao_l[ind];

		assert(evictHash->Insert(pao->key, strlen(pao->key), pao->value, VALUE_SIZE));

		if (evictHash->checkWriteBufFlush()) {                        
                	for (int i=last_flush_ind+1; i<=ind; i++) {
				destroyPAO(pao_l[i]);
                	}                                            
			last_flush_ind = ind;
                }
		ind++;
	}
	free(pao_l);

	/* next bit of code tests whether the input stage has completed. If
	   it has, and the number of tokens processed by the input stage is
	   equal to the number processed by this one, then it also adds all
	   the PAOs in the hash table to the list and clears the hashtable
           essentially flushing all global state. */

	tokens_processed++;
	// if the number of buffers > 1 then some might be queued up
	if (aggregator->input_finished && tokens_processed == 
			aggregator->tot_input_tokens) {
		for (typename Hash::iterator it = hashtable.begin(); 
				it != hashtable.end(); it++) {
			if (evict_list_ctr >= evict_list_size) {
				evict_list_size += LIST_SIZE_INCR;
				if (call_realloc(&evicted_list, evict_list_size) == NULL) {
					perror("realloc failed");
					return NULL;
				}
			}
			assert(evict_list_ctr < evict_list_size);
			evicted_list[evict_list_ctr++] = it->second;
		}	
		hashtable.clear();
	}

	if (evict_list_ctr >= evict_list_size) {
		evict_list_size++;
		if (call_realloc(&evicted_list, evict_list_size) == NULL) {
			perror("realloc failed");
			return NULL;
		}
	}
	assert(evict_list_ctr < evict_list_size);
	evicted_list[evict_list_ctr] = emptyPAO;
	return evicted_list;
}

#endif // LIB_EXTERNALHASHER_H