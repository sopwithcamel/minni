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
#include "MapperAggregator.h"
#include "Util.h"

#define HT_CAPACITY		500000

/**
 * - Consumes: array of PAOs to be aggregated
 * - Produces: array of PAOs evicted from in-memory HT 
 * 
 * TODO:
 * - overload += operator in user-defined PartialAgg class
 * - pass in partitioning function as a parameter
 * - change to TBB hash_map which is parallel access
 */

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
class Hasher : public tbb::filter {
public:
	Hasher(MapperAggregator* agg, PartialAgg* emptyPAO, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~Hasher();
	void (*destroyPAO)(PartialAgg* p);
	void setFlushOnComplete();
private:
	MapperAggregator* aggregator;
	typedef std::tr1::unordered_map<KeyType, PartialAgg*, HashAlgorithm, EqualTest> Hash;
	PartialAgg* emptyPAO;
	PartialAgg** evicted_list;
	size_t ht_size;
	Hash hashtable;
	uint64_t tokens_processed;
	bool flush_on_complete;
	void* operator()(void* pao_list);
};

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
Hasher<KeyType, HashAlgorithm, EqualTest>::Hasher(MapperAggregator* agg, 
			PartialAgg* emptyPAO,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		ht_size(0),
		flush_on_complete(false),
		tokens_processed(0)
{
}

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
Hasher<KeyType, HashAlgorithm, EqualTest>::~Hasher()
{
	typename Hash::iterator it;
	for (it = hashtable.begin(); it != hashtable.end(); it++) {
		free(it->first);
	}
	hashtable.clear();
}

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
void* Hasher<KeyType, HashAlgorithm, EqualTest>::operator()(void* pao_list)
{
	char *key, *value;
	PartialAgg** pao_l = (PartialAgg**)pao_list;
	std::pair<typename Hash::iterator, bool> result;
	size_t ind = 0;
	PartialAgg* pao;

	size_t evict_list_ctr = 0;
	size_t evict_list_size = 1;
	evicted_list = (PartialAgg**)malloc(sizeof(PartialAgg*));

	// PAO to be evicted next. We maintain pointer because begin() on an unordered
	// map is an expensive operation!
	typename Hash::iterator next_evict = hashtable.end();

	while (strcmp((pao_l[ind])->key, emptyPAO->key)) { 
		pao = pao_l[ind];
		result = hashtable.insert(std::make_pair(pao->key, pao));
		if (!result.second) { // the insertion didn't occur
			(result.first->second)->merge(pao);
			destroyPAO(pao);
		} else { // the PAO was inserted
			if (ht_size == HT_CAPACITY) { // PAO has to be evicted
				if (next_evict == hashtable.end())
					next_evict = hashtable.begin();
				typename Hash::iterator evict_el = next_evict;
				next_evict++;

				if (evict_list_ctr >= evict_list_size) {
					evict_list_size += LIST_SIZE_INCR;
					if (call_realloc(&evicted_list, evict_list_size) == NULL) {
						perror("realloc failed");
						return NULL;
					}
				}
				assert(evict_list_ctr < evict_list_size);
				evicted_list[evict_list_ctr++] = evict_el->second;
				hashtable.erase(evict_el);				
			} else {
				ht_size++;
			}
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
	if (flush_on_complete || (aggregator->input_finished && 
			tokens_processed == aggregator->tot_input_tokens)) {
		fprintf(stderr, "Hasher: input finished %d!\n", hashtable.size());
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

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
void Hasher<KeyType, HashAlgorithm, EqualTest>::setFlushOnComplete()
{
	flush_on_complete = true;
}
#endif // LIB_INTERNALHASHER_H
