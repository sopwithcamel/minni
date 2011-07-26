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
class InternalHasher : public tbb::filter {
public:
	InternalHasher(PartialAgg* emptyPAO, void (*destroyPAOFunc)(PartialAgg* p));
	~InternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	typedef std::tr1::unordered_map<KeyType, PartialAgg*, HashAlgorithm, EqualTest> Hash;
	PartialAgg* emptyPAO;
	PartialAgg** evicted_list;
	size_t ht_size;
	Hash hashtable;
	void* operator()(void* pao_list);
};

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
InternalHasher<KeyType, HashAlgorithm, EqualTest>::InternalHasher(PartialAgg* emptyPAO, void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),
		emptyPAO(emptyPAO),
		destroyPAO(destroyPAOFunc),
		ht_size(0)
{
}

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
InternalHasher<KeyType, HashAlgorithm, EqualTest>::~InternalHasher()
{
	typename Hash::iterator it;
	for (it = hashtable.begin(); it != hashtable.end(); it++) {
		free(it->first);
	}
	hashtable.clear();
}

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
void* InternalHasher<KeyType, HashAlgorithm, EqualTest>::operator()(void* pao_list)
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

	while (strcmp((pao_l[ind])->key, EMPTY_KEY)) { 
		pao = pao_l[ind];
		result = hashtable.insert(std::make_pair(pao->key, pao));
		if (!result.second) { // the insertion didn't occur
			(result.first->second)->merge(pao);
			free(pao->key);
			free(pao->value);
			destroyPAO(pao);
		} else { // the PAO was inserted
			if (ht_size == HT_CAPACITY) { // PAO has to be evicted
				if (next_evict == hashtable.end())
					next_evict = hashtable.begin();
				typename Hash::iterator evict_el = next_evict;
				next_evict++;

				if (evict_list_ctr >= evict_list_size) {
					PartialAgg** tmp;
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

#endif // LIB_INTERNALHASHER_H
