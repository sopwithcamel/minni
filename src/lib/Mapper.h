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
#include <libconfig.h++>
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
#include "Aggregator.h"

#define GetCurrentDir getcwd

using namespace std;
using namespace tbb;
using namespace workdaemon;
using namespace libconfig;

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
	Mapper(PartialAgg* (*__createPAO)(const char* t), void (*__destroyPAO)(PartialAgg* p));
	~Mapper();
	PartialAgg* (*createPAO)(const char* token); 
	void (*destroyPAO)(PartialAgg* p);
	//vector <ofstream*>  my_file_streams; //TODO actually needed?
	int num_partition;
	Aggregator* aggregs;
	TimeLog tl;
private:
};

//the type of class factories
//typedef Mapper* create_mapper_t();
//typedef void destroy_mapper_t (Mapper*);



class MapperWrapperTask : public task {
  public:
	Mapper* mapper;	
	PartialAgg* (*__libminni_create_pao)(const char* v);
	void (*__libminni_destroy_pao)(PartialAgg* pao);
	string so_path;
	MapInput myinput;
	MapperWrapperTask (JobID jid, Properties * p, TaskRegistry * t, LocalFileRegistry * f);
	task* execute();
	~MapperWrapperTask();
  private:
	Config cfg;
	JobID jobid;
	Properties* prop;
	TaskRegistry* taskreg;
	LocalFileRegistry* filereg;
	int ParseProperties(string& soname, uint64_t& num_partitions);
	int UserMapLinking(const char* soname);
	string GetCurrentPath();
	string GetLocalFilename(string path, JobID jobid, int i);	
};


#endif
