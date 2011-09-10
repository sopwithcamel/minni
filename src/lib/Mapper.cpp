#include "Mapper.h"

#include "BucketAggregator.h"
#include "ExthashAggregator.h"
#include "HashAggregator.h"
#include "HashsortAggregator.h"
#include <dlfcn.h>

//#define lt__PROGRAM__LTX_preloaded_symbols lt_libltdl_LTX_preloaded_symbols

//MapInput Class

uint64_t MapInput::key_value(char** value, ChunkID id) {

	cout<<"Mapper: KDFS: Connecting to KDFS"<<endl;
	KDFS myhdfs(master_name,port);
	cout<<"Mapper: KDFS: The master name is "<<master_name<<endl;
	cout<<"Mapper: KDFS: The port is "<<port<<endl;
	bool conn = myhdfs.connect();
	if(!conn)
		cout<<"Mapper: KDFS: Unable to establish connection :( \n";
	else
		cout<<"Mapper: KDFS: Able to establish connection :) \n";

	cout<<"Mapper:  KDFS: I am looking for file "<<file_location<<endl;
	if(myhdfs.checkExistence(file_location))
		cout<<"Mapper: KDFS: file in location!!\n";
	uint64_t length = myhdfs.getChunkSize(file_location);
	cout<<"Mapper: KDFS: It told me that the chunk size of this file is "<<length<<endl;
/*	
	*value = (char*) malloc(length+1); 
*/

	cout<<"Mapper: KDFS: Going to read chunks from KDFS"<<endl;
	uint64_t offset = id*length;
	int64_t k = myhdfs.readChunkOffset(file_location, offset, *value, length);
	if(k == -1)
		cout<<"Mapper: KDFS: Reading failed! :( "<<endl;
	else
		cout<<"Mapper: KDFS: Read number of blocks: "<<k<<endl;
	myhdfs.closeFile(file_location);
	bool disconn = myhdfs.disconnect();
	if(!disconn)
		cout<<"Mapper: KDFS: Unable to disconnect \n";
	else
		cout<<"Mapper: KDFS: Able to disconnect \n";

	return k;
}

//Mapper
Mapper::Mapper(PartialAgg* (*__createPAO)(const char* t), 
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
	try {
		cfg.readFile(CONFIG_FILE);
	}
	catch (FileIOException e) {
		fprintf(stderr, "Error reading config file \n");
		exit(1);
	}	
	catch (ParseException e) {
		fprintf(stderr, "Error reading config file: %s at %s:%d\n", e.getError(), e.what(), e.getLine());
		exit(1);
	}
}

/* TODO: destroy all PAO's created */
MapperWrapperTask::~MapperWrapperTask()
{
}

int MapperWrapperTask::ParseProperties(string& soname, uint64_t& num_partitions)
{//TODO checking and printing error reports!	
	stringstream ss;
	string soname_string;
	soname = (*prop)["SO_NAME"];
	cout<<"Mapper: soname is "<<soname<<endl;
  	myinput.file_location = (*prop)["FILE_IN"];
	cout<<"Mapper: file location is "<<myinput.file_location<<endl;
	string chunk_temp_start = (*prop)["CID_START"];
	ss <<chunk_temp_start;
	ss >> myinput.chunk_id_start;
	string chunk_temp_end = (*prop)["CID_END"];
	stringstream ss4;
	ss4 <<chunk_temp_end;
	ss4 >> myinput.chunk_id_end;
	cout<<"Mapper: chunk id  start is "<<myinput.chunk_id_start<<endl;
	cout<<"Mapper: chunk id end is "<<myinput.chunk_id_end<<endl;
  	myinput.master_name = (*prop)["DFS_MASTER"];
	cout<<"Mapper: dfs master is "<<myinput.master_name<<endl;
        string port_temp = (*prop)["DFS_PORT"];
	stringstream ss2;
	ss2 <<port_temp;
	uint16_t port_int;
	ss2 >> port_int;
	myinput.port =  port_int;
	cout<<"Mapper: port - the string version is "<<(*prop)["DFS_PORT"]<<endl;
	cout<<"Mapper: port is -converted version "<<myinput.port<<endl;
	string part = (*prop)["NUM_REDUCERS"];
	stringstream ss3;
	ss3 << part;
	ss3 >> num_partitions; 
	cout<<"Mapper: number of partitions - the string version is "<<(*prop)["NUM_REDUCERS"]<<endl;
	cout<<"Mapper: number of partions is converted version"<<num_partitions<<endl;
	return 0;	
}

int MapperWrapperTask::UserMapLinking(const char* soname)  { //TODO link Partial aggregates also
	const char* err;
	void* handle;
	fprintf(stdout, "Given path: %s\n", soname);
//	LTDL_SET_PRELOADED_SYMBOLS();
	handle = dlopen(soname, RTLD_LAZY);
	if (!handle) {
		fputs (dlerror(), stderr);
		return 1;
	}

	__libminni_create_pao = (PartialAgg* (*)(const char*)) dlsym(handle, "__libminni_pao_create");
	if ((err = dlerror()) != NULL)
	{
		fprintf(stderr, "Error locating symbol __libminni_create_pao in %s\n", err);
		exit(-1);
	}
	__libminni_destroy_pao = (void (*)(PartialAgg*)) dlsym(handle, "__libminni_pao_destroy");
	if ((err = dlerror()) != NULL)
	{
		fprintf(stderr, "Error locating symbol __libminni_destroy_pao in %s\n", err);
		exit(-1);
	}
	mapper = new Mapper(__libminni_create_pao, __libminni_destroy_pao);

	return 0;
}

string MapperWrapperTask::GetCurrentPath() {
	char cCurrentPath[FILENAME_MAX];
        if (!GetCurrentDir(cCurrentPath, sizeof(cCurrentPath)))
        {
                return 0;  //TODO change here!!!
        }
        cCurrentPath[sizeof(cCurrentPath) - 1] = '\0'; /* not really required */
        string path;
        path = cCurrentPath;
	cout<<"Mapper: current path is "<<path<<endl;
	return path;
}

string MapperWrapperTask::GetLocalFilename(string path, JobID jobid, int i) {
	stringstream ss;
       	ss << path;
        ss << "/job";
       	ss << jobid;
//       	ss << "_partition";
       	ss << "_part";
       	ss << i;
	ss << ".map";
	cout<<"The local file name generated is "<<ss.str()<<endl;
        return ss.str();
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
	cout << "Hri: Start of map phase" << asctime(localtime(&ltime));
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

	string selected_map_aggregator;
	try {
		Setting& c_sel_aggregator = cfg.lookup("minni.aggregator.selected.map");
		selected_map_aggregator = (const char*)c_sel_aggregator;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}

	string f_prefix;
	try {
		Setting& c_prefix = cfg.lookup("minni.common.file_prefix");
		f_prefix = (const char*)c_prefix;
	}
	catch (SettingNotFoundException e) {
		fprintf(stderr, "Setting not found %s\n", e.getPath());
	}

	string map_out_file = "mapfile";
	stringstream ss;
	ss << jobid;
	map_out_file += ss.str() + "-part";

	if (!selected_map_aggregator.compare("simple")) {
		mapper->aggregs = dynamic_cast<Aggregator*>(new HashAggregator(
						&cfg, Map, npart, &myinput, 
						NULL, mapper->createPAO, 
						mapper->destroyPAO, map_out_file.c_str()));
	} else if (!selected_map_aggregator.compare("bucket")) {
		mapper->aggregs = dynamic_cast<Aggregator*>(new BucketAggregator(
						&cfg, Map, npart, &myinput, NULL,
						mapper->createPAO, mapper->destroyPAO, 
						map_out_file.c_str()));
	} else if (!selected_map_aggregator.compare("exthash")) {
		mapper->aggregs = dynamic_cast<Aggregator*>(new ExthashAggregator(
						&cfg, Map, npart, &myinput, NULL,
						mapper->createPAO, mapper->destroyPAO,
						map_out_file.c_str()));
	} else if (!selected_map_aggregator.compare("hashsort")) {
		mapper->aggregs = dynamic_cast<Aggregator*>(new HashsortAggregator(
						&cfg, Map, npart, &myinput, NULL,
						mapper->createPAO, mapper->destroyPAO,
						map_out_file.c_str()));
	} else {
		fprintf(stderr, "Illegel aggregator chosen!\n");
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
	ltime = time(NULL);
	cout << "Hri: End of map phase" << asctime(localtime(&ltime));
	mapper->tl.dumpLog();
	
	delete mapper->aggregs;
	delete mapper;	
	
	filereg->recordComplete(my_Filelist);
	taskreg->setStatus(jobid, jobstatus::DONE);

	return NULL;
}
