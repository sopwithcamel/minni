#ifndef LIB_CONCURRENTHASHFILTER_H
#define LIB_CONCURRENTHASHFILTER_H

#include <stdlib.h>
#include <iostream>

#include "tbb/blocked_range.h"
#include "tbb/concurrent_hash_map.h"
#include "tbb/parallel_for.h"
#include "tbb/pipeline.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"
#include "tbb/tick_count.h"

#include "Aggregator.h"
#include "HashUtil.h"
#include "PartialAgg.h"
#include "Util.h"

class ConcurrentHashInserter
{
    struct HashCompare {
        static size_t hash(const char* key)
        {
            return HashUtil::BobHash(key, strlen(key), 42);
        }
        //! True if strings are equal
        static bool equal(const char* s1, const char* s2) {
            return (s1 && s2 && strcmp(s1, s2) == 0);
        }
    };
    typedef tbb::concurrent_hash_map<const char*, PartialAgg*, HashCompare> Hashtable;

    struct Aggregate {
        Hashtable* ht;
        bool destroyMerged_;
        void (*destroyPAO_)(PartialAgg* p);

        Aggregate(Hashtable* ht_, void (*destroyPAOFunc)(PartialAgg* p),
                bool destroy) : 
            ht(ht_),
            destroyMerged_(destroy),
            destroyPAO_(destroyPAOFunc) {}
        void operator()(const tbb::blocked_range<PartialAgg*> r) const
        {
            for (PartialAgg* it=r.begin(); it != r.end(); ++it) {
                Hashtable::accessor a;
                if (ht->insert(a, it->key())) { // wasn't present
                    a->second = it;
                } else { // already present
                    a->second->merge(it);
                    if (destroyMerged_)
                        destroyPAO_(it);
                }
            }
        }
    };
  public:
	ConcurrentHashInserter(Aggregator* agg,
            size_t (*createPAOFunc)(Token* t, PartialAgg** p),
			void (*destroyPAOFunc)(PartialAgg* p),
            int num_part,
			const size_t max_keys);
	~ConcurrentHashInserter();
	void* operator()(void* pao_list);
  private:
	size_t next_buffer;
	MultiBuffer<FilterInfo>* send_;
    MultiBuffer<PartialAgg*>* evicted_list_;
	size_t (*createPAO_)(Token* t, PartialAgg** p);
	void (*destroyPAO_)(PartialAgg* p);

    Hashtable* ht;
};
#endif //LIB_CONCURRENTHASHFILTER_H
