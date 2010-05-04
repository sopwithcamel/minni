#ifndef Mapper_H
#define Mapper_H
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
#include <map>
#include "PartialAgg.h"
#define GetCurrentDir getcwd

using namespace std;
using namespace tbb;
using namespace workdaemon;

class MapperWrapperTask;
class MapInput {
  friend class MapperWrapperTask;
  public:
	MapInput() {};
	~MapInput() {};
	virtual string key_value(); 
  private:
	ChunkID chunk_id;   
	URL file_location;	
	uint16_t port;
	string master_name;
};

class Mapper {
  public:
	Mapper() {};
	~Mapper();
	virtual void Map (MapInput* mapinp){} ; //will be overloaded
	virtual void Emit (string key, string value);
	virtual int GetPartition (string key); //the default parititioner. This can be overriden
	//vector <ofstream*>  my_file_streams; //TODO actually needed?
	int num_partition;
	vector<Aggregator*> aggregs;

};

//the type of class factories
typedef Mapper* create_mapper_t();
typedef void destroy_mapper_t (Mapper*);



class MapperWrapperTask : public task {
  public:
	create_mapper_t* create_fn;
	destroy_mapper_t* destroy_fn;
	create_partialagg_t* create_agg_fn;
	destroy_partialagg_t* destroy_agg_fn;
	MapInput myinput;
	MapperWrapperTask (JobID jid, Properties * p, TaskRegistry * t, LocalFileRegistry * f);
	task* execute();
  private:
	JobID jobid;
	Properties* prop;
	TaskRegistry* taskreg;
	LocalFileRegistry* filereg;
	int ParseProperties(string& soname, int& num_partitions);
	int UserMapLinking(string soname);
	string get_current_path();
	string get_local_filename(string path, JobID jobid, int i);	
};


#endif
