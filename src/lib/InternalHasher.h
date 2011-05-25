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

/**
 * To be used as the final stage in the pipeline.
 * - Consumes vector< pair<char*, int> > 
 * - Produces: nil
 * 
 * TODO:
 * - overload += operator in user-defined PartialAgg class
 * - change to TBB hash_map which is parallel access
 */

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
class InternalHasher : public tbb::filter {
public:
	InternalHasher(void (*destroyPAOFunc)(PartialAgg* p));
	~InternalHasher();
	void (*destroyPAO)(PartialAgg* p);
private:
	typedef std::tr1::unordered_map<KeyType, PartialAgg*, HashAlgorithm, EqualTest> Hash;
	Hash hashtable;
	void* operator()(void* pao_list);
};

template <typename KeyType, typename HashAlgorithm, typename EqualTest>
InternalHasher<KeyType, HashAlgorithm, EqualTest>::InternalHasher(void (*destroyPAOFunc)(PartialAgg* p)) :
		filter(/*serial=*/true),
		destroyPAO(destroyPAOFunc)
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

	while (strcmp((pao_l[ind])->key, EMPTY_KEY)) { 
		pao = pao_l[ind];
		result = hashtable.insert(std::make_pair(pao->key, pao));
		if (!result.second) { // the insertion didn't occur
			(result.first->second)->merge(pao);
			free(pao->key);
			free(pao->value);
			destroyPAO(pao);
			// TODO: explicitly call destructor?
		}
		ind++;
	}
	free(pao_l);
}

#endif // LIB_INTERNALHASHER_H
