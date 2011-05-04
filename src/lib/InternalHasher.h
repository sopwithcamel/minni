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

/**
 * To be used as the final stage in the pipeline.
 * - Consumes vector< pair<char*, int> > 
 * - Produces: nil
 * 
 * TODO:
 * - overload += operator in user-defined PartialAgg class
 * - change to TBB hash_map which is parallel access
 */

template <typename KeyType, typename ValueType, typename HashAlgorithm, typename EqualTest>
class InternalHasher : public tbb::filter {
public:
	InternalHasher();
	~InternalHasher();
private:
	typedef std::tr1::unordered_map<KeyType, ValueType, HashAlgorithm, EqualTest> Hash;
	typedef std::vector< std::pair<KeyType, ValueType> > KVVector;
	Hash hashtable;
	void* operator()(void* kv_vector);
};

template <typename KeyType, typename ValueType, typename HashAlgorithm, typename EqualTest>
InternalHasher<KeyType, ValueType, HashAlgorithm, EqualTest>::InternalHasher() :
		filter(/*serial=*/true)
{
}

template <typename KeyType, typename ValueType, typename HashAlgorithm, typename EqualTest>
InternalHasher<KeyType, ValueType, HashAlgorithm, EqualTest>::~InternalHasher()
{
	typename Hash::iterator it;
	for (it = hashtable.begin(); it != hashtable.end(); it++) {
		free(it->first);
	}
	hashtable.clear();
}

template <typename KeyType, typename ValueType, typename HashAlgorithm, typename EqualTest>
void* InternalHasher<KeyType, ValueType, HashAlgorithm, EqualTest>::operator()(void* kv_vector)
{
	KVVector* kv = (KVVector*)kv_vector;
	std::pair<typename Hash::iterator, bool> result;
	typename KVVector::iterator it;
	for (it = (*kv).begin(); it != (*kv).end(); it++) { 
		result = hashtable.insert(std::make_pair(it->first, it->second));
		if (!result.second) { // the insertion didn't occur
			(result.first->second)->merge(it->second);
			free(it->first);
		}
	}
	(*kv).clear();
}

#endif // LIB_INTERNALHASHER_H
