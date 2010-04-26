#include "WorkDaemon.h"
#include "WorkDaemon_other.h"
#include "tbb/task.h"

using namespace workdaemon;
using namespace tbb;
using namespace std;


#define SCAN_FREQUENCY 5

class MapperTask: public task{
 public:	
  JobID jid;
  ChunkID cid;
  JobStatusMap * status_map;
  MapperTask(JobID jid_, ChunkID cid_, JobStatusMap * status_map_);
  task * execute();
};

class ReducerTask: public task{
 public:	
  JobID jid;
  PartitionID pid;
  string outfile;
  JobStatusMap * status_map;
  ReducerTask(JobID jid_, PartitionID pid_, string outfile_, JobStatusMap * status_map_);
  task * execute();
};

typedef concurrent_hash_map<JobID,MapperTask*,HashCompare<JobID> > JobMapperMap;
typedef concurrent_hash_map<JobID,ReducerTask*,HashCompare<JobID> > JobReducerMap;

class MasterTask: public task{
 public:
  JobStatusMap * status_map;
  JobMapperMap * mapper_map;
  JobReducerMap * reducer_map;
  MasterTask(JobStatusMap * smap_, JobMapperMap * mmap_, JobReducerMap * rmap_);
  task * execute();
};
