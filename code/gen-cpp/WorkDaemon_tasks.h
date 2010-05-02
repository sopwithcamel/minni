#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"
#include "WorkDaemon_file.h"

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

#define WORKER_PORT 9090

using namespace workdaemon;
using namespace tbb;
using namespace std;


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
  TaskRegistry * tasks;
  LocalFileRegistry * files;
  Properties * prop;
  MapperTask(JobID jid_, 
	     Properties * p, 
	     TaskRegistry * t,
	     LocalFileRegistry * f);
  ~MapperTask();
  task * execute();
};

class ReducerTask: public task{
 public:	
  JobID jid;
  TaskRegistry * tasks;
  GrabberMap * grab;
  Properties * prop;
  ReducerTask(JobID jid_, 
	      Properties * p,
	      TaskRegistry * t,
	      GrabberMap * g);
  ~ReducerTask();
  task * execute();
};


