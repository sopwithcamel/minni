#include "Mapper.h"



//MapInput Class

string MapInput::key_value() {
	
		//TODO: need to read the DFS reads!
	

}

//Mapper
Mapper::~Mapper() {
	for (int i = 0; i < my_file_streams.size(); i++)
        {
                delete my_file_streams[i];
        }
}

void Mapper::Emit (string key, string value) { //Partial aggregation going on here
	
	//Case1: New key value - insert into map
	int i = GetPartition(key);
	Aggregator::iterator found = (aggregs[i])->find(key);
	if(found == (aggregs[i])->end()) {
		(*aggregs[i])[key] = new PartialAgg(key,value);
	}
	//Existing key value - then add to the partial result
	else {
		(*aggregs[i])[key]->add(value);
	}
}

int Mapper::GetPartition (string key) {//, int key_size) {
	unsigned long hash = 5381;
	char* str =  (char*) key.c_str();
	int key_size = key.length();
	int i;	
	for (i = 0; i < key_size; i++)
	{
		hash = ((hash << 5) + hash) + ((int) str[i]);
	}
	return hash % num_partition;
}

//Mapper wrapper task

MapperWrapperTask::MapperWrapperTask (JobID jid, Properties * p, TaskRegistry * t, LocalFileRegistry * f) {
	jobid = jid;
	prop = p;
	taskreg = t;
	filereg = f;
}

int MappperWrapperTask::ParseProperties(string& soname, int& num_partitions) {//TODO checking and printing error reports!	
	stringstream ss;
	soname = (*prop)["SO_NAME"];
  	myinput.file_location = (*prop)["FILE_IN"];
	string chunk_temp = (*prop)["CID"];
	ss(chunk_temp);
	ss >> myinput.chunk_id;
  	myinput.master_name = (*prop)["DFS_MASTER"];
        string port_temp = (*prop)["DFS_PORT"];
        ss(port_temp);
	int port_int;
	ss >> port_int;
	myinput.port = (uint16_t) port_int;
	string part = (*prop)["NUM_REDUCERS"];
	ss(part);
	ss >> num_partition; 
	return 0;
}

int MapperWrapperTask::UserMapLinking(string soname)  { //TODO link Partial aggregates also
	void* wordcount =  dlopen(soname.c_str(), RTLD_LAZY);
	if(!wordcount)	{
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return 1;
	}
	
	//load the symbols
	create_fn = (create_mapper_t*) dlsym(wordcount, "create_mapper");	
	destroy_fn = (destroy_mapper_t*) dlsym(wordcount, "destroy_mapper");

	if(!create_fn || !destroy_fn) {
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return 1;
	}
	return 0;
}

string MapperWrapperTask::GetCurrentPath() {
	char cCurrentPath[FILENAME_MAX];
        if (!GetCurrentDir(cCurrentPath, sizeof(cCurrentPath)))
        {
                return 0;
        }
        cCurrentPath[sizeof(cCurrentPath) - 1] = '\0'; /* not really required */
        string path;
        path = cCurrentPath;
	return path;
}

string MapperWrapperTask::GetLocalFilename(string path, JobID jobid, int i) {
	stringstream ss;
       	ss << path;
        ss << "/";
       	ss << jobid;
       	ss << "_";
       	ss << i;
        return ss.str();
}

task* MapperWrapperTask::execute() {
	string soname;
	int npart;
	if(ParseProperties(soname,npart) == 1)  {
		return NULL; //TODO check with Erik
	}
	//dynamically loading the classes
	if(UserMapLinking(soname) == 1) {
		return NULL; //TODO check with Erik
	}

	//instantiating my mapper 	
	Mapper* my_mapper = create_fn();
	my_mapper->num_partitions = npart;

	for(int i = 0; i < npart; i++)
	{
		my_mapper->aggregs.push_back(new Aggregator);
	}
	my_mapper->Map(*myinput);

	string path = GetCurrentPath();

	//now i need to start writing into file
	for(int i = 0; i < npart ; i++)
	{
	       	string final_path = GetLocalFilename(path,jobid,i);
	 	ofstream temp = new ofstream();
       		ofstream.open(final_path.c_str());
		//TODO should do serializing one by one here
		
		//freeing up all  resources
        	ofstream.close();
	}


	taskreg->setStatus(jobid, jobstatus::DONE);
	//TODO:Also has to pass the file pointers / update them

	


	
	return NULL;

	
	
}








