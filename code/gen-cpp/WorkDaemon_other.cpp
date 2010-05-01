#include "WorkDaemon_tasks.h"
#include <fstream>
#include <sstream>
#include <iostream>

#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

using namespace std;

#define SCAN_FREQUENCY 5

class MapperTask: public task{
 public:	
  JobID jid;
  ChunkID cid;
  TaskRegistry * tasks;
  MapperTask(JobID jid_, ChunkID cid_, TaskRegistry * tasks_);
  task * execute();
};

class ReducerTask: public task{
 public:	
  JobID jid;
  PartitionID pid;
  string outfile;
  TaskRegistry * tasks;
  ReducerTask(JobID jid_, PartitionID pid_, string outfile_, TaskRegistry * tasks_);
  task * execute();
};

class MasterTask: public task{
 private:
  TaskRegistry * tasks;
 public:
  MasterTask(TaskRegistry * tasks_);
  task * execute();
};

TaskRecord::TaskRecord(JobID j, task* t, JobKind k, Status s){
  jid = j;
  task_ptr = t;
  kind = k;
  status = s;
}

string TaskRecord::toString(){
  stringstream ss;
  ss << "(" << jid << ", " << kind << ", " << status << ")";
  return ss.str();
}

void TaskRegistry::addJob(JobID jid, task * ptr, JobKind jk){
  assert(!this->exists(jid));
  // Get the accessor
  TaskMap::accessor acc_task;
  this->task_map.insert(acc_task, jid);

  // Fill out the record
  acc_task->second = TaskRecord(jid, ptr, jk, jobstatus::INPROGRESS);
}

bool TaskRegistry::exists(JobID jid){
  TaskMap::const_accessor acc_task;
  bool found = this->task_map.find(acc_task, jid);
  return found;
}

Status TaskRegistry::getStatus(JobID jid){
  TaskMap::const_accessor acc_task;
  bool found = this->task_map.find(acc_task, jid);
  if(!found){
    return jobstatus::DNE;
  }
  // Have the reported status. If it claims to be running, make sure
  Status status = acc_task->second.status;
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

void TaskRegistry::setStatus(JobID jid, Status status){
  assert(this->exists(jid));
  TaskMap::accessor acc_stat;
  this->task_map.find(acc_stat, jid);
  acc_stat->second.status = status;
}

void TaskRegistry::remove(JobID jid){
  this->task_map.erase(jid);
}

void TaskRegistry::cullReported(){
  assert(false);
}

void TaskRegistry::getReport(Report &report){
  report.clear();
  TaskMap::range_type range = this->task_map.range();
  for(TaskMap::iterator it = range.begin(); it !=range.end(); it++){
    JobID jid = it->first;
    Status status = it->second.status;
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
   ss << "TaskReg = [\n";
  for(TaskMap::iterator it = range.begin(); it !=range.end(); it++){
    ss << "\t" << it->second.toString() << "\n";
  }
  ss << "]\n";
  return ss.str();
}

string printReport(map<JobID,Status> &M){
  bool first = true;
  stringstream ss;
  ss << "[";
  for(map<JobID,Status>::iterator it = M.begin(); it != M.end(); it++){
    if(!first){
      ss << ", ";
      first = false;
    }
    ss << "(" << it->first << "->" << it->second << ")";
  }
  ss << "]";
  return ss.str();
}
