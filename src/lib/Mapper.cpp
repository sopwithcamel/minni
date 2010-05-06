#include "Mapper.h"



//MapInput Class

int MapInput::key_value(char** value) {

	cout<<"Mapper: Connecting to HDFS"<<endl;
	HDFS myhdfs(master_name,port);
	myhdfs.connect();
	if(myhdfs.checkExistence(file_location))
		cout<<"file not in location!!\n";
	uint64_t length = myhdfs.getChunkSize(file_location);
	*value = (char*) malloc(length+1);
	cout<<"Reading chunks from HDFS"<<endl;
	int k = myhdfs.readChunkOffset(file_location, (uint64_t) 0, *value, length);
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

void Map (MapInput* input) {
	cout<<"Mapper: entered the map phase\n";
	cout<<"Mapper: I will be reading from HDFS soon\n";
	
		char* text;
		
                int n = input->key_value(&text);

	cout<<"Mapper: I have read from HDFS\n";
                for(int i = 0; i < n; ) {
                        //skip through the leading whitespace
                        while((i < n) && isspace(text[i]))
                                i++;

                        //Find word end
                        int start = i;
                        while ((i < n) && !isspace(text[i]))
                                i++;
                        //if(start < i)
                                //Emit();
                }
	cout<<"Mapper: Done with map job\n";
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

int MapperWrapperTask::ParseProperties(string& soname, uint64_t& num_partitions) {//TODO checking and printing error reports!	
	stringstream ss;
	soname = (*prop)["SO_NAME"];
	cout<<"Mapper: soname is "<<soname<<endl;
  	myinput.file_location = (*prop)["FILE_IN"];
	cout<<"Mapper: file location is "<<myinput.file_location<<endl;
	string chunk_temp = (*prop)["CID"];
	ss <<chunk_temp;
	ss >> myinput.chunk_id;
	cout<<"Mapper: chunk id is "<<myinput.chunk_id<<endl;
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
	cout<<"Mapper: current path is "<<path<<endl;
	return path;
}

string MapperWrapperTask::GetLocalFilename(string path, JobID jobid, int i) {
	stringstream ss;
       	ss << path;
        ss << "/";
       	ss << jobid;
       	ss << "_";
       	ss << i;
	cout<<"The local file name generated is "<<ss.str()<<endl;
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
	uint64_t npart;
	if(ParseProperties(soname,npart) == 1)  { //TODO
		cout<<"Parse properties something wrong. I am leaving!"<<endl;
		return NULL; 
	}
	
	cout<<"Mapper: Parse happened properly! "<<endl;
	//dynamically loading the classes
	//if(UserMapLinking(soname) == 1) { //TODO
	//	cout<<"User map linking not happening very successfully!"<<endl;
	//	return NULL; 
	//}
	cout<<"Mapper: User map too is successful "<<endl;
	//instantiating my mapper 
	cout<<"Mapper: Instantiating the mapper \n";	
	Mapper* my_mapper = new Mapper();
	cout<<"Mapper: Setting the number of partitions on the mapper to "<<my_mapper->num_partition<<endl;
	my_mapper->num_partition = npart;
	
	cout<<"The number of partitions that it gets is "<<npart<<"\n";
	//my_mapper->num_partition = 10;
	//npart = 10;
	cout<<"Mapper: starting to push back the aggregators\n";
	for(int i = 0; i < npart; i++)
	{
		my_mapper->aggregs.push_back(new Aggregator);
	}
	cout<<"Mapper: I am going to run map here"<<endl;

	my_mapper->Map(&myinput);

	cout<<"Mapper: Supposedly done with mapping"<<endl;
	string path = GetCurrentPath();
	vector<File> my_Filelist;
	cout<<"Mapper: About to start writing into files and my npart is "<<npart<<"\n";
	//now i need to start writing into file
	for(int i = 0; i < npart ; i++)
	{
		cout<<"Mapper: Executing loop for i = "<<i<<endl;
		string final_path = GetLocalFilename(path,jobid,i);
		cout<<"Mapper: I am going to write the file "<<final_path<<endl;
	 	FILE* fptr = fopen(final_path.c_str(), "w");
		cout<<"I should have opened the file "<<final_path<<endl;
		Aggregator::iterator aggiter;
		for(aggiter = (my_mapper->aggregs[i])->begin(); aggiter != (my_mapper->aggregs[i])->end(); ++aggiter)
		{
			string k = aggiter->first;
			PartialAgg* curr_par = aggiter->second;
			string val = curr_par->value;
			char type = 1;
			serialize(fptr, type, uint64_t (k.size()), k.c_str(), uint64_t (val.size()), val.c_str());
			
		}
		cout<<"Mapper: I am closing the file "<<final_path<<endl;
		fclose(fptr);
		cout<<"Mapper: Going to tell the workdaemon about the file \n";
		File f1(jobid, i, final_path);
		cout<<"Pushed back the file to worker daemon list \n";
		my_Filelist.push_back(f1);

	}
	

	
	filereg->recordComplete(my_Filelist);
	taskreg->setStatus(jobid, jobstatus::DONE);

	return NULL;

	
	
}








