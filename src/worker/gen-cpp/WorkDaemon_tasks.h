#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"
#include "WorkDaemon_file.h"


// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

using namespace workdaemon;
using namespace tbb;
using namespace std;

class TaskRecord {
 public:
  JobID jid;
  task * task_ptr; 
  JobKind kind;
  JobStatus status;
  TaskRecord(JobID j=0, task* t=NULL, JobKind k= jobkind::NIL, JobStatus s=jobstatus::DNE);
  string toString();
};

typedef concurrent_hash_map<JobID, TaskRecord> TaskMap;

// Maintains a list of all tasks, and their statuses
class TaskRegistry{
 private:
  TaskMap task_map;
 public:
  void addJob(JobID jid, task* ptr, JobKind jk);
  bool exists(JobID jid);
  
  JobStatus getStatus(JobID jid);
  void setStatus(JobID jid, JobStatus status);
  void remove(JobID jid);

  bool mapper_still_running();
  void cullReported();
  void getReport(Report &report);
  string toString();

  void clear();
};

//Tasks

class MapperTestTask: public task{
 public:	
  JobID jid;
  TaskRegistry * tasks;
  LocalFileRegistry * files;
  Properties * prop;
  MapperTestTask(JobID jid_, 
	     Properties * p, 
	     TaskRegistry * t,
	     LocalFileRegistry * f);
  ~MapperTestTask();
  task * execute();
};

class ReducerTestTask: public task{
 public:	
  JobID jid;
  TaskRegistry * tasks;
  GrabberRegistry * grab;
  Properties * prop;
  ReducerTestTask(JobID jid_, 
	      Properties * p,
	      TaskRegistry * t,
	      GrabberRegistry * g);
  ~ReducerTestTask();
  task * execute();
};


