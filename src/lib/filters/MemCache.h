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

enum QueryType {WORD, FIL};

class MemCache {
public:
	MemCache(const char* query_file, QueryType typ);
	~MemCache();
	size_t size();
	char* getItem(uint64_t i);
	char* getFileContents(uint64_t i);
private:
	std::vector<char*> queryList;
	std::vector<char*> queryContents;
	char* queryFile;
	QueryType queryType;
	void loadFileCache();
	void loadWordCache();
	void readFileContents();
};

#endif // LIB_MEMCACHE_H
