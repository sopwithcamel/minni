#include <iostream>
#include "WorkDaemon.h"
#include "WorkDaemon_other.h"
#include "WorkDaemon_tasks.h"

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

using namespace tbb;
using namespace std;


// Master Task
MasterTask::MasterTask( JobStatusMap * smap_, JobMapperMap * mmap_):
  status_map(smap_), mapper_map(mmap_)
{}

task * MasterTask::execute(){
  // 1) Start the loop
  while(true){
    this_tbb_thread::sleep(tick_count::interval_t((double)1));
    // 2) Wake up, scan all map tasks
    for(JobMapperMap::iterator it = this->mapper_map->begin();
	it != this->mapper_map->end(); it++){
      JobStatusMap::accessor status;
      // 2a) Ensure that there is a corresponding status
      bool found = this->status_map->find(status, it->first);
      if(!found){
	cerr << "No status found for " << it->first << "!" << endl;
	continue;
      }
      // 2b) Get the reported status, and the actual status.
      state_type state = it->second->state();
      bool believe_running = status->second == TASK_INPROGRESS;
      bool actually_running = state == executing;
      cout << "Job " 
	   << it->first  
	   << " reports " 
	   << believe_running 
	   << " and is actually " 
	   << actually_running << endl;
      // 2c) Check to make sure that the statuses make sense
      if(believe_running && !actually_running){
	cerr << "\tThis is bad, master should be informed..." << endl;
	status->second = TASK_DEAD;
	status.release();
      }
    }
  }
  return NULL;
}

// Mapper Task

MapperTask::MapperTask(JobID jid_, ChunkID cid_, JobStatusMap * status_map_):
  jid(jid_), cid(cid_), status_map(status_map_)
{}

task * MapperTask::execute(){
  // 1) Work for a while
  this_tbb_thread::sleep(tick_count::interval_t((double)5)); // "Work"
  generate_keyvalue_pairs("hello", 3);

  // 2) Report that you're done.
  StatusAccessor a;
  this->status_map->insert(a, jid);
  printf("Status of %d before: %d\n", (int)a->first, (int)a->second);
  a->second = TASK_COMPLETE;
  printf("Status of %d after: %d\n", (int)a->first, (int)a->second);
  return NULL;
}
