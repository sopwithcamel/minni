#include "WorkDaemon.h"
#include "WorkDaemon_other.h"
#include "tbb/task.h"

using namespace workdaemon;
using namespace tbb;
using namespace std;


typedef concurrent_hash_map<JobID,Status,HashCompare<JobID> > JobStatusMap;
typedef JobStatusMap::accessor StatusAccessor;

class MapperTask: public task{
public:	
	JobID jid;
	ChunkID cid;
	JobStatusMap * status_map;
	MapperTask(JobID jid_, ChunkID cid_, JobStatusMap * status_map_);
	task * execute();
};

typedef concurrent_hash_map<JobID,MapperTask*,HashCompare<JobID> > JobMapperMap;

class MasterTask: public task{
public:
	JobStatusMap * status_map;
	JobMapperMap * mapper_map;
	MasterTask(JobStatusMap * smap_, JobMapperMap * mmap_);
	task * execute();
};

