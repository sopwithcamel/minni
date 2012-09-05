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

#include "AccumulatorFilter.h"
#include "Aggregator.h"
#include "HashUtil.h"
#include "PartialAgg.h"
#include "Util.h"

class ConcurrentHashInserter : public AccumulatorInserter
{
    struct HashCompare {
        static size_t hash(const char* key)
        {
            size_t a = HashUtil::BobHash(key, strlen(key), 42);
            return a;
        }
        //! True if strings are equal
        static bool equal(const char* s1, const char* s2) {
            return (s1 && s2 && strcmp(s1, s2) == 0);
        }
    };
    typedef tbb::concurrent_hash_map<const char*, PartialAgg*,
            HashCompare> Hashtable;

    struct Aggregate {
        Hashtable* ht;
        bool destroyMerged_;
        const Operations* const ops;

        Aggregate(Hashtable* ht_, bool destroy,
                const Operations* const ops) :
            ht(ht_),
            destroyMerged_(destroy),
            ops(ops) {}
        void operator()(const tbb::blocked_range<PartialAgg**> r) const
        {
            for (PartialAgg** it=r.begin(); it != r.end(); ++it) {
                Hashtable::accessor a;
                if (ht->insert(a, ops->getKey(*it))) { // wasn't present
                    a->second = *it;
                } else { // already present
                    ops->merge(a->second, *it);
                    if (destroyMerged_)
                        ops->destroyPAO(*it);
                }
/*
                const char* key = ops->getKey(*it);
                std::pair<Hashtable::iterator, bool> ret = ht->insert(
                        std::make_pair<const char*, PartialAgg*>(key, *it));
                if (!ret.second) { // not inserted
                    ops->merge(ret.first->second, *it);
                    if (destroyMerged_)
                        ops->destroyPAO(*it);
                }
*/
            }
        }
    };
  public:
	ConcurrentHashInserter(Aggregator* agg,
            const Config &cfg,
			const size_t max_keys);
	~ConcurrentHashInserter();
	void* operator()(void* pao_list);
  private:
    Hashtable* ht_;
	size_t next_buffer;
	MultiBuffer<FilterInfo>* send_;
    MultiBuffer<PartialAgg*>* evicted_list_;

    Hashtable::const_iterator evict_it;
    size_t num_evicted;
    size_t to_be_evicted;
    bool to_be_cleared_;
};
#endif //LIB_CONCURRENTHASHFILTER_H
