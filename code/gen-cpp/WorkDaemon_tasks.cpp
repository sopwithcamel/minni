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
// Basically scans through all the jobs every so often and checks
// if their still alive
MasterTask::MasterTask( JobStatusMap * smap_, JobMapperMap * mmap_, JobReducerMap * rmap_):
  status_map(smap_), mapper_map(mmap_), reducer_map(rmap_)
{}

task * MasterTask::execute(){
  while(true){
    // 1) Sleep for a bit. Why not, you've earned it.
    this_tbb_thread::sleep(tick_count::interval_t((double)SCAN_FREQUENCY));

    // 2) Wake up, scan all map tasks
    JobMapperMap::range_type map_range = mapper_map->range();
    for(JobMapperMap::iterator it = map_range.begin();
	it != map_range.end(); it++){
      // 2a) Find the corresponding status for the mapper
      JobStatusMap::accessor status;
      bool found = this->status_map->find(status, it->first);
      assert(found);

      // 2b) Get the reported status, and the actual status.
      state_type state = it->second->state();
      bool believe_running = status->second == JobStatus::INPROGRESS;
      bool actually_running = state == executing;

      // 2c) Check to make sure that the statuses make sense
      if(believe_running && !actually_running){
	status->second = JobStatus::DEAD; 
	//Seems like the thread terminated early; mark
      }

      // 2c) Release the accessor
      status.release();
    }
    // 3) Repeat for Reducers
    JobReducerMap::range_type reduce_range = reducer_map->range();
    for(JobReducerMap::iterator it = reduce_range.begin();
	it != reduce_range.end(); it++){
      // 2a) Find the corresponding status for the reducer
      JobStatusMap::accessor status;
      bool found = this->status_map->find(status, it->first);
      assert(found);

      // 2b) Get the reported status, and the actual status.
      state_type state = it->second->state();
      bool believe_running = status->second == JobStatus::INPROGRESS;
      bool actually_running = state == executing;

      // 2c) Check to make sure that the statuses make sense
      if(believe_running && !actually_running){
	status->second = JobStatus::DEAD; 
	//Seems like the thread terminated early; mark
      }

      // 2c) Release the accessor
      status.release();
    }
  }
  return NULL;
}

// Mapper Task
// Runs the code
MapperTask::MapperTask(JobID jid_, ChunkID cid_, JobStatusMap * status_map_):
  jid(jid_), cid(cid_), status_map(status_map_)
{}

task * MapperTask::execute(){
  // 1) Work for a while
  this_tbb_thread::sleep(tick_count::interval_t((double)1)); // "Work"
  //generate_keyvalue_pairs("hello", 3);

  // 2) Report that you're done.
  JobStatusMap::accessor a;
  this->status_map->insert(a, jid);
  a->second = JobStatus::DONE;
  return NULL;
}

// Mapper Task
// Runs the code
ReducerTask::ReducerTask(JobID jid_, PartitionID pid_, string outfile_, JobStatusMap * status_map_):
  jid(jid_), pid(pid_), outfile(outfile_), status_map(status_map_)
{}

task * ReducerTask::execute(){
  // 1) Work for a while
  this_tbb_thread::sleep(tick_count::interval_t((double)1)); // "Work"

  // 2) Report that you're done.
  JobStatusMap::accessor a;
  this->status_map->insert(a, jid);
  a->second = JobStatus::DONE;
  return NULL;
}
