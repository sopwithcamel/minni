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
#include "Aggregator.h"
#include "KDFS.h"
#include "Defs.h"
#include "Util.h"
#include "util.h"

#define GetCurrentDir getcwd
#define ERROR_EXIT 1
#define SUCCESS_EXIT 0
#define BASE_SLEEPTIME 2
#define EXPONENT 1.5
#define MAX_SLEEPTIME 10
#define MAX_STRING_LENGTH 100000

using namespace libconfig;

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
	Reducer(Operations* __ops);
	~Reducer();
    Operations* ops;
  	Aggregator* aggreg;
};


class ReducerWrapperTask : public task {
  public:
	ReducerWrapperTask (JobID jid, Properties * p, TaskRegistry * t, GrabberRegistry * g);
	task* execute ();
	ReduceOutput myoutput;
	Operations* __libminni_operations;
	
  private:
	Config cfg;
	Reducer* reducer;
	JobID jobid;
	Properties * prop;
	TaskRegistry * taskreg;
	GrabberRegistry* grabreg;
	PartID my_partition;
	int ParseProperties(string& soname);
	int UserMapLinking(const char* soname);  
	string Write(string result_key, string result_value);
	
};

#endif
