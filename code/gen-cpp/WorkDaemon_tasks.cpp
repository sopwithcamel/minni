#include "WorkDaemon_tasks.h"
#include <fstream>
#include <sstream>
#include <iostream>

#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

using namespace std;

MapperTask::MapperTask(JobID jid_, 
		       Properties * p, 
		       TaskRegistry * t,
		       LocalFileRegistry * f):
  jid(jid_), prop(p), tasks(t), files(f){}

MapperTask::~MapperTask(){
  if(prop != NULL){
    delete prop;
    prop = NULL;
  }
}

task * MapperTask::execute(){
  //tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(4.0));
  tasks->setStatus(jid, jobstatus::DONE);
  return NULL;
}

ReducerTask::ReducerTask(JobID jid_, 
			 Properties * p,
			 TaskRegistry * t,
			 GrabberRegistry * g):
  jid(jid_), prop(p), tasks(t),grab(g){}

ReducerTask::~ReducerTask(){
  if(prop != NULL){
    delete prop;
    prop = NULL;
  }
}

task * ReducerTask::execute(){
  //tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t(4.0));
  tasks->setStatus(jid, jobstatus::DONE);
  return NULL;
}

TaskRecord::TaskRecord(JobID j, task* t, JobKind k, JobStatus s){
  jid = j;
  task_ptr = t;
  kind = k;
  status = s;
}


string TaskRecord::toString(){
  stringstream ss;
  ss << "(j" << jid << ", k" << kind << ", s" << status << ")";
  return ss.str();
}

// Record that this new job is running
void TaskRegistry::addJob(JobID jid, task * ptr, JobKind jk){
  if(this->exists(jid)){
    assert(!this->exists(jid));
    return;
  }
  // Get the accessor
  TaskMap::accessor acc_task;
  this->task_map.insert(acc_task, jid);

  // Fill out the record
  acc_task->second = TaskRecord(jid, ptr, jk, jobstatus::INPROGRESS);
}

// Do we know about this job?
bool TaskRegistry::exists(JobID jid){
  TaskMap::const_accessor acc_task;
  bool found = this->task_map.find(acc_task, jid);
  return found;
}

// Figure out whether the job is running or dead
// Checks if the job status claims that it is running
// DNE -> no knowledge of this job
// INPROGRESS -> Job is running
// DONE -> Job is not running, exited gracefully
// DONE_AND_REPORTED -> DONE, and told the master
// DEAD -> Job is not running, did not exit gracefully
// DEAD_AND_REPORTED -> DEAD, and told the master
JobStatus TaskRegistry::getStatus(JobID jid){
  TaskMap::const_accessor acc_task;
  bool found = this->task_map.find(acc_task, jid);
  if(!found){
    return jobstatus::DNE;
  }
  // Have the reported status. If it claims to be running, make sure
  JobStatus status = acc_task->second.status;
  if(status == jobstatus::INPROGRESS){
    bool actually_running = (acc_task->second.task_ptr->state() == task::executing);
    if(!actually_running){
      status = jobstatus::DEAD;  
      acc_task.release(); // Release the accessor so we can write      
      this->setStatus(jid, status);
    }
  }
  return status;
}

// Update the status; tasks use this to mark a file as done
void TaskRegistry::setStatus(JobID jid, JobStatus status){
  assert(this->exists(jid));
  TaskMap::accessor acc_stat;
  this->task_map.find(acc_stat, jid);
  acc_stat->second.status = status;
}

// Yanks some job
void TaskRegistry::remove(JobID jid){
  this->task_map.erase(jid);
}

// Are any of the mapper still running, i.e. could we be still generating data?
bool TaskRegistry::mapper_still_running(){
  TaskMap::range_type range = this->task_map.range();
  for(TaskMap::iterator it = range.begin(); it !=range.end(); it++){
    JobID jid = it->first;
    JobStatus status = this->getStatus(jid);
    JobKind kind = it->second.kind;
    if(status == jobstatus::INPROGRESS 
       && kind == jobkind::MAPPER){
      return true;
    }
  }
  return false;
}


// Yank any entries that we have told the master about.
void TaskRegistry::cullReported(){
  TaskMap::range_type range = this->task_map.range();
  for(TaskMap::iterator it = range.begin(); it !=range.end();){
    JobID jid = it->first;
    JobStatus status = it->second.status;
    it++;
    if(status == jobstatus::DONE_AND_REPORTED){
      this->task_map.erase(jid);
    }
  }
}

// Tell the master about anything that is done or dead
void TaskRegistry::getReport(Report &report){
  report.clear();
  TaskMap::range_type range = this->task_map.range();
  for(TaskMap::iterator it = range.begin(); it !=range.end(); it++){
    JobID jid = it->first;
    JobStatus status = this->getStatus(jid);
    if(status == jobstatus::DONE){
      report[jid] = jobstatus::DONE;
      it->second.status = jobstatus::DONE_AND_REPORTED;
    }
    if(status == jobstatus::DEAD){
      report[jid] = jobstatus::DEAD;
      it->second.status = jobstatus::DEAD_AND_REPORTED;
    }
  }
}

string TaskRegistry::toString(){
  stringstream ss;
  TaskMap::range_type range = this->task_map.range();
  ss << "[\n";
  for(TaskMap::iterator it = range.begin(); it !=range.end(); it++){
    ss << "\t" << it->second.toString() << "\n";
  }
  ss << "]\n";
  return ss.str();
}

string printReport(Report &M){
  bool first = true;
  stringstream ss;
  ss << "[";
  for(Report::iterator it = M.begin(); it != M.end(); it++){
    if(!first){
      ss << ", ";
      first = false;
    }
    ss << "(" << it->first << "->" << it->second << ")";
  }
  ss << "]";
  return ss.str();
}
