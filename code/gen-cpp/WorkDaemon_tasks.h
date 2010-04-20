#include "WorkDaemon.h"

#include "tbb/task.h"
#include "tbb/concurrent_hash_map.h"


#define TASK_INPROGRESS 0
#define TASK_COMPLETE 1
#define TASK_DEAD 2

using namespace workdaemon;
using namespace tbb;
using namespace std;

struct BasicHashCompare { 
	static size_t hash( const JobID& x){
		return (size_t) x;
	}
	static bool equal( const JobID& x, const JobID& y ){
		return x == y;
	}
};

typedef concurrent_hash_map<JobID,Status,BasicHashCompare> JobStatusMap;
typedef JobStatusMap::accessor StatusAccessor;

class MapperTask: public task{
public:
	
	JobID jid;
	ChunkID cid;
	JobStatusMap * status_map;
	MapperTask(JobID jid_, ChunkID cid_, JobStatusMap * status_map_);
	task * execute();
};

typedef concurrent_hash_map<JobID,MapperTask*,BasicHashCompare> JobMapperMap;

class MasterTask: public task{
public:
	JobStatusMap * status_map;
	JobMapperMap * mapper_map;
	MasterTask(JobStatusMap * smap_, JobMapperMap * mmap_);
	task * execute();
};
