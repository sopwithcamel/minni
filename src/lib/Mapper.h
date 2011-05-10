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
#include "daemon_types.h"
#include "WorkDaemon_file.h"
#include "WorkDaemon_tasks.h"
#include <ltdl.h>
#include <dlfcn.h>
#include <map>
#include <set>
#include "PartialAgg.h"
#include "TimeLog.h"
#include "KDFS.h"
#include "MapperAggregator.h"

#define REGISTER_MAPPER(x) extern "C" Mapper* __libminni_map_create(PartialAgg* (*__libminni_create_pao)(char*)) {return new x(__libminni_create_pao);} \
	                extern "C" void __libminni_map_destroy(Mapper* m) {delete m;}
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
	virtual uint64_t key_value(char** str, ChunkID id);
	ChunkID chunk_id_start;
	ChunkID chunk_id_end;
	string file_location;	
	uint16_t port;
	string master_name;
};

class Mapper {
  public:
	Mapper() {};
	~Mapper();
	virtual void Map (); //will be overloaded
	//vector <ofstream*>  my_file_streams; //TODO actually needed?
	int num_partition;
	vector<MapperAggregator*> aggregs;
	TimeLog tl;

};

//the type of class factories
//typedef Mapper* create_mapper_t();
//typedef void destroy_mapper_t (Mapper*);



class MapperWrapperTask : public task {
  public:
	Mapper* mapper;	
	PartialAgg* (*__libminni_create_pao)(string v);
	void (*__libminni_destroy_pao)(PartialAgg* pao);
	string so_path;
	lt_dlhandle handle;
	MapInput myinput;
	MapperWrapperTask (JobID jid, Properties * p, TaskRegistry * t, LocalFileRegistry * f);
	task* execute();
	~MapperWrapperTask();
  private:
	JobID jobid;
	Properties* prop;
	TaskRegistry* taskreg;
	LocalFileRegistry* filereg;
	int ParseProperties(string& soname, uint64_t& num_partitions);
	int UserMapLinking(string soname);
	string GetCurrentPath();
	string GetLocalFilename(string path, JobID jobid, int i);	
};


#endif
