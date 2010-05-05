#include "Mapper.h"



//MapInput Class

int MapInput::key_value(char** value) {

	HDFS myhdfs(master_name,port);
	myhdfs.connect();
	if(myhdfs.checkExistence(file_location))
		cout<<"file not in location!!\n";
	uint64_t length = myhdfs.getChunkSize(file_location);
	*value = (char*) malloc(length+1);
	int k = myhdfs.readChunkOffset(file_location, (uint64_t) 0, value, length);
	myhdfs.disconnect();
	return length;
}

//Mapper
Mapper::~Mapper() {
//	for (int i = 0; i < my_file_streams.size(); i++)
  //      {
    //            delete my_file_streams[i];
      //  }
}

void Mapper::Emit (string key, string value) { //Partial aggregation going on here
	
	//Case1: New key value - insert into map
	int i = GetPartition(key);
	
	Aggregator::iterator found = (*(aggregs[i])).find(key);
	if(found == (aggregs[i])->end()) {
		(*(aggregs[i]))[key] = new PartialAgg(value);
	}
	//Existing key value - then add to the partial result
	else {
		(*(aggregs[i]))[key]->add(value);
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

int MapperWrapperTask::ParseProperties(string& soname, int& num_partitions) {//TODO checking and printing error reports!	
	stringstream ss;
	soname = (*prop)["SO_NAME"];
  	myinput.file_location = (*prop)["FILE_IN"];
	string chunk_temp = (*prop)["CID"];
	ss <<chunk_temp;
	ss >> myinput.chunk_id;
  	myinput.master_name = (*prop)["DFS_MASTER"];
        string port_temp = (*prop)["DFS_PORT"];
        ss <<port_temp;
	int port_int;
	ss >> port_int;
	myinput.port = (uint16_t) port_int;
	string part = (*prop)["NUM_REDUCERS"];
	ss << part;
	ss >> num_partitions; 
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
                return 0;  //TODO change here!!!
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

void serialize(FILE* fileOut, char type, uint64_t keyLength, const char* key, uint64_t valueLength, const char* value)
{
        keyLength = keyLength + 1; /* write \0 */
        valueLength = valueLength + 1; /* write \0 */
        fwrite(&type, sizeof(char), 1, fileOut);
        fwrite(&keyLength, sizeof(uint64_t), 1, fileOut);
        fwrite(key, sizeof(char), keyLength, fileOut);
        fwrite(&valueLength, sizeof(uint64_t), 1, fileOut);
        fwrite(value, sizeof(char), valueLength, fileOut);
}



task* MapperWrapperTask::execute() {
	string soname;
	int npart;
	if(ParseProperties(soname,npart) == 1)  { //TODO
		cout<<"Parse properties something wrong. I am leaving!"<<endl;
		return NULL; 
	}
	cout<<"Parse happened properly! "<<endl;
	//dynamically loading the classes
	if(UserMapLinking(soname) == 1) { //TODO
		cout<<"User map linking not happening very successfully!"<<endl;
		return NULL; 
	}
	cout<<"User map too is successful "<<endl;
	//instantiating my mapper 	
	Mapper* my_mapper = create_fn();
	my_mapper->num_partition = npart;

	for(int i = 0; i < npart; i++)
	{
		my_mapper->aggregs.push_back(new Aggregator);
	}
	my_mapper->Map(&myinput);

	string path = GetCurrentPath();
	vector<File> my_Filelist;
	cout<<"About to start writing into files\n";
	//now i need to start writing into file
	for(int i = 0; i < npart ; i++)
	{
	       	string final_path = GetLocalFilename(path,jobid,i);
		File f1;
		f1.name = final_path;	
		my_Filelist.push_back(f1);
	 	FILE* fptr = fopen(final_path.c_str(), "w");
		Aggregator::iterator aggiter;
		for(aggiter = (my_mapper->aggregs[i])->begin(); aggiter != (my_mapper->aggregs[i])->end(); ++aggiter)
		{
			string k = aggiter->first;
			PartialAgg* curr_par = aggiter->second;
			string val = curr_par->value;
			char type = 1;
			serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
			
		}
	}

	
	filereg->recordComplete(my_Filelist);
	taskreg->setStatus(jobid, jobstatus::DONE);

	return NULL;

	
	
}








