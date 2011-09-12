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
	Hasher(Aggregator* agg, PartialAgg* emptyPAO, size_t capacity, 
			void (*destroyPAOFunc)(PartialAgg* p));
	~Hasher();
	void (*destroyPAO)(PartialAgg* p);
	void setFlushOnComplete();
private:
	Aggregator* aggregator;
	typedef std::tr1::unordered_map<KeyType, PartialAgg*, HashAlgorithm, EqualTest> Hash;
	PartialAgg* emptyPAO;
	PartialAgg*** evicted_list;
	size_t ht_size;
	size_t ht_capacity;
	size_t next_buffer;
	Hash hashtable;
	uint64_t tokens_processed;
	bool flush_on_complete;
	void* operator()(void* pao_list);
};

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
Hasher<KeyType, HashAlgorithm, EqualTest>::Hasher(Aggregator* agg, 
			PartialAgg* emptyPAO,
			size_t capacity,
			void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),	/* maintains global state which is not yet concurrent access */
		aggregator(agg),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		ht_size(0),
		ht_capacity(capacity),
		next_buffer(0),
		flush_on_complete(false),
		tokens_processed(0)
{
	uint64_t num_buffers = aggregator->getNumBuffers();
	evicted_list = (PartialAgg***)malloc(sizeof(PartialAgg**) * num_buffers);
}

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
Hasher<KeyType, HashAlgorithm, EqualTest>::~Hasher()
{
	typename Hash::iterator it;
	for (it = hashtable.begin(); it != hashtable.end(); it++) {
		free(it->first);
	}
	hashtable.clear();
	free(evicted_list);
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
	evicted_list[next_buffer] = (PartialAgg**)malloc(sizeof(PartialAgg*) * MAX_KEYS_PER_TOKEN);
	PartialAgg** this_list = evicted_list[next_buffer];
	next_buffer = (next_buffer + 1) % aggregator->getNumBuffers();

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
			if (ht_size == ht_capacity) { // PAO has to be evicted
				if (next_evict == hashtable.end())
					next_evict = hashtable.begin();
				typename Hash::iterator evict_el = next_evict;
				next_evict++;

				this_list[evict_list_ctr++] = evict_el->second;
				assert(evict_list_ctr < MAX_KEYS_PER_TOKEN); 
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
		fprintf(stderr, "Hasher: input finished %zu!\n", hashtable.size());
		for (typename Hash::iterator it = hashtable.begin(); 
				it != hashtable.end(); it++) {
			this_list[evict_list_ctr++] = it->second;
			assert(evict_list_ctr < MAX_KEYS_PER_TOKEN);
		}	
		hashtable.clear();
	}
	this_list[evict_list_ctr] = emptyPAO;
	return this_list;
}

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
void Hasher<KeyType, HashAlgorithm, EqualTest>::setFlushOnComplete()
{
	flush_on_complete = true;
}
#endif // LIB_INTERNALHASHER_H
