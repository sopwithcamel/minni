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
	typedef std::vector<PartialAgg*> PAOVector;
	Hash hashtable;
	void* operator()(void* pao_vector);
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
void* InternalHasher<KeyType, HashAlgorithm, EqualTest>::operator()(void* pao_vector)
{
	char *key, *value;
	PAOVector* paov = (PAOVector*)pao_vector;
	std::pair<typename Hash::iterator, bool> result;
	typename PAOVector::iterator it;
	for (it = (*paov).begin(); it != (*paov).end(); it++) {
		key = (*it)->key;
		value = (*it)->value;
		result = hashtable.insert(std::make_pair(key, *it));
		if (!result.second) { // the insertion didn't occur
			(result.first->second)->merge(*it);
			free(key);
			free(value);
			destroyPAO(*it);
			// TODO: explicitly call destructor?
		}
	}
	(*paov).clear();
}

#endif // LIB_INTERNALHASHER_H
