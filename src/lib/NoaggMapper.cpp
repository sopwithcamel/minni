#include "config.h"
#include "NoaggMapper.h"



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
	*value = (char*) malloc(length+1); /*freed in line 73 Map fn aft using all data that is passed*/
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
Mapper::~Mapper() {
//	for (int i = 0; i < my_file_streams.size(); i++)
  //      {
    //            delete my_file_streams[i];
      //  }
}
 
string Mapper::GetLocalMapDumpFile()
{
	return "/localfs/hamur/mapdumpfile";
}

void Mapper::serialize(FILE* fileOut, string key, string value)
{
        fwrite(key.c_str(), sizeof(char), uint64_t(key.size()), fileOut);
	fwrite(" ", sizeof(char), 1, fileOut);
        fwrite(value.c_str(), sizeof(char), uint64_t(value.size()), fileOut);
	fwrite("\n", sizeof(char), 1, fileOut);
	
}


void Mapper::Map(MapInput* input) {
	cout<<"Mapper: entered the map phase\n";
	cout<<"Mapper: I will be reading from KDFS soon\n";
	string mdf = GetLocalMapDumpFile();
	FILE *fptr = fopen(mdf.c_str(), "w");
	for (ChunkID id = input->chunk_id_start; id <= input->chunk_id_end; id++)
	{
		char* text;
		uint64_t n = input->key_value(&text,id);
		cout<<"Mapper: I have read from KDFS\n";
		unsigned int i;
		for( i = 0; i < n; ) {
		//skip through the leading whitespace
			while((i < n) && isspace(text[i]))
				i++;
			//Find word end
			unsigned int start = i;
			while ((i < n) && !isspace(text[i]))
				i++;
		
			if(start < i)
			{
				//cout<<"Mapper: The word is ";
				string key(&text[start],(i-start));
				//cout<<key;
				//cout<<endl;
				serialize(fptr, key, "1");
			}
        	}
		free(text);
	}
	fclose(fptr);
	// Call nsort on the mapdumpfile
	cout << "Issuing nsort command " << endl;
	stringstream nsortCommand;
	nsortCommand << "nsort ";
	nsortCommand << mdf;
	string sortedMapDumpFileName = "/localfs/hamur/sortedMapOut.txt"; 
	nsortCommand << " -o ";
	nsortCommand << sortedMapDumpFileName;
	system(nsortCommand.str().c_str());

	// Read from sorted file and emit to aggregators

	cout << "Emitting to aggregators" << endl;
	ifstream fstr(sortedMapDumpFileName.c_str());
	char* s_key = (char*)malloc(100);
	char* s_value = (char*)malloc(100);
	while (!fstr.eof()) {
		fstr >> s_key;
		fstr >> s_value;
		Emit(s_key, s_value);
	}
	cout<<"Mapper: Done with map job\n";
}

/* This function will simply write unaggregated key values to local disk
*/
void Mapper::Emit (string key, string value) {
	
	int i = GetPartition(key);
	(*(aggregs[i])).add(key, value);
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

int MapperWrapperTask::UserMapLinking(string soname)  { //TODO link Partial aggregates also
	void* wordcount =  dlopen(soname.c_str(), RTLD_LAZY);
	if(!wordcount)	{
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return 1;
	}
	
	//load the symbols
	//create_fn = (create_mapper_t*) dlsym(wordcount, "create_mapper");	
	//destroy_fn = (destroy_mapper_t*) dlsym(wordcount, "destroy_mapper");

	//if(!create_fn || !destroy_fn) {
	//	taskreg->setStatus(jobid, jobstatus::DEAD);
	//	return 1;
	//}
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
       	ss << "_partition";
       	ss << i;
	ss << ".map";
	cout<<"The local file name generated is "<<ss.str()<<endl;
        return ss.str();
}



task* MapperWrapperTask::execute() {
	string soname;
	uint64_t npart;
	if(ParseProperties(soname,npart) == 1)  { //TODO
		cout<<"Parse properties something wrong. I am leaving!"<<endl;
		return NULL; 
	}
	
	time_t ltime = time(NULL);
	cout << "Hri: Start of map phase" << asctime(localtime(&ltime));
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
	my_mapper->num_partition = npart;
	cout<<"Mapper: Setting the number of partitions to "<<my_mapper->num_partition<<endl;
	
	cout<<"The number of partitions that it gets is "<<npart<<"\n";
	//my_mapper->num_partition = 10;
	//npart = 10;
	cout<<"Mapper: starting to push back the aggregators\n";
	for(unsigned int i = 0; i < npart; i++)
	{
//		my_mapper->aggregs.push_back(new MapperAggregator());
		my_mapper->aggregs.push_back(new MapperAggregator(100000, i));
	}
	cout<<"Mapper: I am going to run map here"<<endl;
	

	my_mapper->Map(&myinput);

	cout<<"Mapper: Supposedly done with mapping"<<endl;
	string path = "/localfs/hamur/";
	vector<File> my_Filelist;
	cout<<"Mapper: About to start writing into files and my npart is "<<npart<<"\n";
	//now i need to start writing into file
	for(unsigned int i = 0; i < npart ; i++)
	{
		cout<<"Mapper: Executing loop for i = "<<i<<endl;
		string final_path = GetLocalFilename(path,jobid,i);
		cout<<"Mapper: I am going to write the file "<<final_path<<endl;

// Call to finalize: merge the final contents of hashtable with most recent dumpfile
		my_mapper->aggregs[i]->finalize(final_path);
		
		cout<<"Mapper: Going to tell the workdaemon about the file \n";
		File f1(jobid, i, final_path);
		cout<<"Pushed back the file to worker daemon list \n";
		my_Filelist.push_back(f1);

	}
	ltime = time(NULL);
	cout << "Hri: End of map phase" << asctime(localtime(&ltime));
	
	for(unsigned int i = 0; i < npart ; i++)
        {
		my_mapper->aggregs[i]->clear();
		delete my_mapper->aggregs[i];
	}

	delete my_mapper;	

	
	filereg->recordComplete(my_Filelist);
	taskreg->setStatus(jobid, jobstatus::DONE);

	return NULL;

	
	
}








