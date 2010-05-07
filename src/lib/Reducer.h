#ifndef Reducer_H
#define Reducer_H
#include <iostream>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include "tbb/task.h"
#include "tbb/tbb_thread.h"
#include <unistd.h>
#include <sstream>
#include <fstream>
#include "daemon_types.h"
#include "WorkDaemon_file.h"
#include "WorkDaemon_tasks.h"
#include <dlfcn.h>
#include "PartialAgg.h"
#include "HDFS.h"
#define GetCurrentDir getcwd
#define ERROR_EXIT 1
#define SUCCESS_EXIT 0
#define BASE_SLEEPTIME 2
#define EXPONENT 1.5
#define MAX_SLEEPTIME 10
#define MAX_STRING_LENGTH 100000


class ReduceTaskWrapper;
class ReduceOutput { 
  friend class ReduceTaskWrapper;
  public:
	ReduceOutput() { };
	~ReduceOutput() { };
	//virtual int Write(string result_key, string result_value);
	string path;
	uint16_t port;
	string master_name;
};

class Reducer {
  public:
	Reducer() {};
	~Reducer(){};
	void AddKeyVal(string key, string value);
	void AddPartialAgg(string key, PartialAgg* pagg);
  	Aggregator* aggreg;
};


class ReducerWrapperTask : public task {
  public:
	ReducerWrapperTask (JobID jid, Properties * p, TaskRegistry * t, GrabberRegistry * g);
	task* execute ();
	ReduceOutput myoutput;
	//create_partialagg_t* create_agg_fn;
	//destroy_partialagg_t* destroy_agg_fn;
	
  private:
	Reducer* my_reducer;
	JobID jobid;
	Properties * prop;
	TaskRegistry * taskreg;
	GrabberRegistry* grabreg;
	PartID my_partition;
	int ParseProperties(string& soname);
	int UserMapLinking(string soname);  
	string GetCurrentPath();
	string GetLocalFilename(string path, JobID jobid);
	void DoReduce(string filename);
	string Write(string result_key, string result_value);
	
};

#endif
