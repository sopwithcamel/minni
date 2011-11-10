#ifndef LIB_MEMCACHE_H
#define LIB_MEMCACHE_H

#include <stdlib.h>
#include <assert.h>
#include <iostream>

#include "tbb/pipeline.h"
#include "tbb/tick_count.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/tbb_allocator.h"

#include "Defs.h"
#include "PartialAgg.h"
#include "Aggregator.h"
#include "Util.h"

class MemCache {
public:
	MemCache(const char* query_file, const char* query_type);
	~MemCache();
	char* operator[](size_t ind);
	size_t size();
private:
	std::vector<char*> queryList;
	char* queryFile;
	enum QueryType {WORD, IMAGE};
	QueryType queryType;
	void loadImageCache();
	void loadWordCache();
};

#endif // LIB_MEMCACHE_H
