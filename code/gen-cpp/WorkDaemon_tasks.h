#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

using namespace workdaemon;
using namespace tbb;
using namespace std;

// Status
namespace jobstatus {enum {DNE, INPROGRESS, DONE, DONE_AND_REPORTED, DEAD, DEAD_AND_REPORTED};}
namespace jobkind {enum {NIL, MAPPER, REDUCER};}

template <class T>
struct HashCompare {
  static size_t hash( const T& x){
    return (size_t) x;
  }
  static bool equal( const T& x, const T& y ){
    return x == y;
  }
};


// Task Registry
string printReport(map<JobID,Status> &M);
typedef unsigned int JobKind;

class TaskRecord {
 public:
  JobID jid;
  task * task_ptr; 
  JobKind kind;
  Status status;
  TaskRecord(JobID j=0, task* t=NULL, JobKind k= jobkind::NIL, Status s=jobstatus::DNE);
  string toString();
};

typedef concurrent_hash_map<JobID, TaskRecord> TaskMap;
typedef map<JobID, Status> Report;

class TaskRegistry{
 private:
  TaskMap task_map;
 public:
  void addJob(JobID jid, task* ptr, JobKind jk);
  bool exists(JobID jid);
  
  Status getStatus(JobID jid);
  void setStatus(JobID jid, Status status);
  void remove(JobID jid);

  bool mapper_still_running();
  void cullReported();
  void getReport(Report &report);
  string toString();
};

//Tasks

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
  PartID pid;
  string outfile;
  TaskRegistry * tasks;
  ReducerTask(JobID jid_, PartID pid_, string outfile_, TaskRegistry * tasks_);
  task * execute();
};


