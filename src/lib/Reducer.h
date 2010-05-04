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
#include "gen-cpp/daemon_types.h"
#include "gen-cpp/WorkDaemon_file.h"
#include "gen-cpp/WorkDaemon_tasks.h"
#include <dlfcn.h>
#define GetCurrentDir getcwd



class ReduceTaskWrapper;
class ReduceOutput { 
  friend class ReduceTaskWrapper;
  public:
	ReduceOutput() { };
	~ReduceOutput() { };
	virtual int Write(string result_key, string result_value);
  private:
	string path;
	uint16_t port;
	string master_name;
};

class Reducer {
  public:
	Reducer();
	~Reducer();
	void AddKeyVal(string key, string value);
	void AddPartialAgg(string key, PartialAgg* pagg);
  	Aggregator* aggreg;
};


class ReducerWrapperTask : public task {
  public:
	ReducerWrapperTask (JobID jid, Properties * p, TaskRegistry * t, GrabberRegistry * g);
	task* execute ();
	MapOutput myoutput;
	create_partialagg_t* create_agg_fn;
	destroy_partialagg_t* destroy_agg_fn;
  private:
	JobID jobid;
	Properties * prop;
	TaskRegistry * taskreg;
	GrabberRegistry* grabreg;
	int my_partition;
	int ParseProperties(string& soname);
	int UserMapLinking(string soname);  
	string get_current_path();
	string get_local_filename(string path, JobID jobid);
};

#endif
