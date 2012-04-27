#include "config.h"
#include "Mapper.h"

#include "BucketAggregator.h"
#include "HashsortAggregator.h"
#include <dlfcn.h>

//Mapper
Mapper::Mapper(size_t (*__createPAO)(Token* t, PartialAgg** p), 
			void (*__destroyPAO)(PartialAgg* p)):
		createPAO(__createPAO),
		destroyPAO(__destroyPAO)	
{
}

Mapper::~Mapper()
{
}


//Mapper wrapper task

MapperWrapperTask::MapperWrapperTask (JobID jid, Properties * p, 
			TaskRegistry * t, LocalFileRegistry * f):
		jobid(jid),
		prop(p),
		taskreg(t),
		filereg(f)
{
	openConfigFile(cfg);
	Setting& c_input_type = readConfigFile(cfg, "minni.input_type");
	string inp_type = (const char*)c_input_type;

	if (!inp_type.compare("chunk"))
		myinput = new ChunkInput();
	else if (!inp_type.compare("file"))
		myinput = new FileInput();
	else if (!inp_type.compare("filechunk"))
		myinput = new FileInput();
}

MapperWrapperTask::~MapperWrapperTask()
{
	delete myinput;
}

int MapperWrapperTask::ParseProperties(string& soname, uint64_t& num_partitions)
{
	myinput->ParseProperties(prop);

	string soname_string;
	soname = (*prop)["SO_NAME"];
	cout<<"Mapper: soname is "<<soname<<endl;

	string part = (*prop)["NUM_REDUCERS"];
	stringstream ss3;
	ss3 << part;
	ss3 >> num_partitions; 
	cout<<"Mapper: num_partitions:"<<num_partitions<<endl;
	return 0;	
}

int MapperWrapperTask::UserMapLinking(const char* soname)
{ 
	const char* err;
	void* handle;
	fprintf(stdout, "Given path: %s\n", soname);
//	LTDL_SET_PRELOADED_SYMBOLS();
	handle = dlopen(soname, RTLD_LAZY);
	if (!handle) {
		fputs (dlerror(), stderr);
		return 1;
	}

	__libminni_create_pao = (size_t (*)(Token*, PartialAgg**)) dlsym(
			handle, "__libminni_pao_create");
	if ((err = dlerror()) != NULL)
	{
		fprintf(stderr, "Error locating symbol __libminni_create_pao\
				in %s\n", err);
		exit(-1);
	}
	__libminni_destroy_pao = (void (*)(PartialAgg*)) dlsym(handle, 
			"__libminni_pao_destroy");
	if ((err = dlerror()) != NULL)
	{
		fprintf(stderr, "Error locating symbol __libminni_destroy_pao\
			in %s\n", err);
		exit(-1);
	}
	mapper = new Mapper(__libminni_create_pao, __libminni_destroy_pao);

	return 0;
}

task* MapperWrapperTask::execute() {
	string soname;
	char *s_name;
	uint64_t npart;

	if(ParseProperties(soname,npart) == 1)  { //TODO
		cout<<"Parse properties something wrong. I am leaving!"<<endl;
		return NULL; 
	}
	s_name = (char*)malloc(soname.length() + 1);
	strcpy(s_name, soname.c_str());
	fprintf(stderr, "SO name: %s\n", s_name);
	time_t ltime = time(NULL);
	cout<<"Mapper: Parse happened properly! "<<endl;
	//dynamically loading the classes
	if(UserMapLinking(s_name) == 1) { //TODO
		cout<<"User map linking not happening very successfully!"<<endl;
		return NULL; 
	}
	free(s_name);
	cout<<"Mapper: User map too is successful "<<endl;

	mapper->num_partition = npart;
	cout<<"Mapper: Setting the number of partitions to "<<mapper->num_partition<<endl;
	
	cout<<"The number of partitions that it gets is "<<npart<<"\n";
	cout<<"Mapper: starting to push back the aggregators\n";

	Setting& c_sel_aggregator = readConfigFile(cfg, "minni.aggregator.selected.map");
	string selected_map_aggregator = (const char*)c_sel_aggregator;

	Setting& c_prefix = readConfigFile(cfg, "minni.common.file_prefix");
	string f_prefix = (const char*)c_prefix;

	string map_out_file = "mapfile";
	stringstream ss;
	ss << jobid;
	map_out_file += ss.str() + "-part";

	TimeLog::addTimeStamp(jobid, "Start of map phase");

	if (!selected_map_aggregator.compare("bucket")) {
		mapper->aggregs = dynamic_cast<Aggregator*>(new BucketAggregator(
						cfg, jobid, Map, npart, myinput, NULL,
						mapper->createPAO, mapper->destroyPAO, 
						map_out_file.c_str()));
	} else if (!selected_map_aggregator.compare("hashsort")) {
		mapper->aggregs = dynamic_cast<Aggregator*>(new HashsortAggregator(
						cfg, jobid, Map, npart, myinput, NULL,
						mapper->createPAO, mapper->destroyPAO,
						map_out_file.c_str()));
	} else {
		fprintf(stderr, "Illegal aggregator chosen!\n");
		exit(1);
	}
	cout<<"Mapper: I am going to run map here"<<endl;
	mapper->aggregs->runPipeline();
	cout<<"Mapper: Supposedly done with mapping"<<endl;
	vector<File> my_Filelist;
	cout<<"Mapper: About to start writing into files and my npart is "<<npart<<"\n";
	//now i need to start writing into file
	for(unsigned int i = 0; i < npart ; i++)
	{
		cout<<"Mapper: Going to tell the workdaemon about the file \n";
		string f = f_prefix + map_out_file;
		stringstream ss;
		ss << i;
		f += ss.str();
		File f1(jobid, i, f.c_str());
		cout<<"Pushed back the file to worker daemon list \n";
		my_Filelist.push_back(f1);
	}
	TimeLog::addTimeStamp(jobid, "End of map phase");
	TimeLog::dumpLog();
	
	delete mapper->aggregs;
	delete mapper;	
	
	filereg->recordComplete(my_Filelist);
	taskreg->setStatus(jobid, jobstatus::DONE);

	return NULL;
}
