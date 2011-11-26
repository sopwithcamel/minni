#ifndef LIB_MAPPER_H
#define LIB_MAPPER_H
#include <iostream>
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
#include "KDFS.h"
#include "MapInput.h"
#include "Aggregator.h"
#include "Util.h"
#include "util.h"

using namespace std;
using namespace tbb;
using namespace workdaemon;
using namespace libconfig;

class Mapper {
public:
	Mapper(size_t (*__createPAO)(Token* t, PartialAgg** p), 
			void (*__destroyPAO)(PartialAgg* p));
	~Mapper();
	size_t (*createPAO)(Token* token, PartialAgg** p); 
	void (*destroyPAO)(PartialAgg* p);
	int num_partition;
	Aggregator* aggregs;
private:
};

class MapperWrapperTask : public task {
  public:
	Mapper* mapper;	
	size_t (*__libminni_create_pao)(Token* t, PartialAgg** p);
	void (*__libminni_destroy_pao)(PartialAgg* pao);
	string so_path;
	MapInput* myinput;
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
};

#endif
