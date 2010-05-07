#include "Reducer.h"

//ReduceOutput class


//Reducer class
void Reducer::AddKeyVal(string key, string value) {
	Aggregator::iterator found = (aggreg)->find(key);
	if(found == (aggreg)->end()) {
		(*aggreg)[key] = new PartialAgg(value);
	}
	//Existing key value - then add to the partial result
	else {
		(*aggreg)[key]->add(value);
	}
}

void Reducer::AddPartialAgg(string key, PartialAgg* pagg) {
	Aggregator::iterator found = (aggreg)->find(key);
	if(found == (aggreg)->end()) {
		(*aggreg)[key] = new PartialAgg(pagg->get_value());
	}
	//Existing key value - then add to the partial result
	else {
		(*aggreg)[key]->merge(pagg);
	}
}



//Reducer Wrapper
ReducerWrapperTask::ReducerWrapperTask (JobID jid, Properties * p, TaskRegistry * t, GrabberRegistry * g) {
	jobid = jid;
	prop = p;
	taskreg = t;
	grabreg = g;
}

int ReducerWrapperTask::ParseProperties(string& soname) {//TODO checking and printing error reports!	
	stringstream ss;
	soname = (*prop)["SO_NAME"];
  	myoutput.path = (*prop)["FILE_OUT"];
	string chunk_temp = (*prop)["PID"];
	ss <<chunk_temp;
	ss >> my_partition;
	myoutput.master_name = (*prop)["DFS_MASTER"];
        string port_temp = (*prop)["DFS_PORT"];
	cout<<"Reducer: The port number as --raw string-- is "<<port_temp<<endl;
	stringstream ss1;
	uint16_t port_int;
	ss1 << port_temp;
	ss1 >> port_int;
	cout<<"Reducer: The port numbeer --parsed-- that i have is "<<port_int<<endl;
	myoutput.port = port_int;
	
	return SUCCESS_EXIT;
}

int ReducerWrapperTask::UserMapLinking(string soname) {
	
	return SUCCESS_EXIT;
}

string ReducerWrapperTask::GetCurrentPath() {
	char cCurrentPath[FILENAME_MAX];
        if (!GetCurrentDir(cCurrentPath, sizeof(cCurrentPath)))
        {
		cout<<"Invalid reducer! "<<endl;
               //TODO 
        }
        cCurrentPath[sizeof(cCurrentPath) - 1] = '\0'; /* not really required */
        string path;
        path = cCurrentPath;
	return path;
}

string ReducerWrapperTask::GetLocalFilename(string path, JobID jobid) {
	stringstream ss;
       	ss << path;
	ss << "/job";
       	ss << jobid;
	ss << ".reduce";
       	return ss.str();
}


void deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
{
        fread(type, sizeof(char), 1, fileIn);
        fread(keyLength, sizeof(uint64_t), 1, fileIn);
        *key = (char*) malloc(*keyLength);
        fread(*key, sizeof(char), *keyLength, fileIn);
        fread(valueLength, sizeof(uint64_t), 1, fileIn);
        *value = (char*) malloc(*valueLength);
        fread(*value, sizeof(char), *valueLength, fileIn);
	cout<<"Reducer: The values are key= "<<*key<<" value= "<<*value<<endl;
}


void ReducerWrapperTask::DoReduce(string filename) {
	cout<<"I am here to do reduce on "<<filename<<endl;
	FILE* fptr = fopen(filename.c_str(),"r");
	char type;
	string key1, value1;
	//adding values
	while(!feof(fptr))//not end of file //TODO check
	{
		cout<<"Inside and now going to read the file "<<endl;
		char *key2, *value2;
		uint64_t keylength, valuelength;
		deSerialize(fptr, &type, &keylength, &key2, &valuelength, &value2);
		if(type == 2)
		{
			cout<<"Type is key value and I am adding "<<"("<<key2<<" "<<value2<<"key value pair\n";
			my_reducer->AddKeyVal(key2,value2);
		}
		else if (type == 1)
		{
			cout<<"Type is partial aggregate and i am adding "<<"("<<key2<<" "<<value2<<"partial aggregator\n";
			
			PartialAgg* newpagg = new PartialAgg(value2);
			my_reducer->AddPartialAgg(key2, newpagg);
		}
	}
	fclose(fptr);
}

string ReducerWrapperTask::Write(string result_key, string result_value) {
	string out = result_key + " " + result_value + "\n";
}




task* ReducerWrapperTask::execute() {
	string soname;
	cout<<"Going to call Parse properties \n";
	if(ParseProperties(soname) == ERROR_EXIT)  {
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return NULL;
	}
	cout<<"After parse properties\n";
	//dynamically loading the classes
	if(UserMapLinking(soname) == ERROR_EXIT) {
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return NULL;
	}
	my_reducer = new Reducer();
	my_reducer->aggreg = new Aggregator();
		
	//filename for the results
	string curr_path = GetCurrentPath();
	string filename = GetLocalFilename(curr_path,jobid);

	int sleeptime = BASE_SLEEPTIME;
	grabreg->setupGrabber(my_partition); //setting up the grabber
	PartStatus curr_stat = grabreg->getStatus(my_partition);
	while(curr_stat != partstatus::DONE && curr_stat != partstatus::DNE)	{
		cout<<"Reducer: Current status for partition"<<my_partition<<"is "<<curr_stat<<endl;
		if(curr_stat == partstatus::BLOCKED)
		{
			if(sleeptime <= MAX_SLEEPTIME)
				sleeptime*=EXPONENT;
			
			tbb::this_tbb_thread::sleep(tbb::tick_count::interval_t((double)sleeptime));		
		}
		else if(curr_stat == partstatus::READY)
		{
			cout<<"Reducer: Reached Ready state!! \n";
			grabreg->getMore(my_partition, filename);
			cout<<"Reducer: Going to do reduce on the file "<<filename<<endl;
			DoReduce(filename);
			cout<<"Reducer: Done with reducing \n";
			sleeptime = BASE_SLEEPTIME;
		}
		
		curr_stat = grabreg->getStatus(my_partition);
		
	}

	cout<<"Reducer: The master is "<<myoutput.master_name<<endl;
	cout<<"Reducer: The output port is "<<myoutput.port<<endl;

	HDFS myhdfs(myoutput.master_name,myoutput.port);
	
	cout<<"Reducer: Opening the HDFS\n";
	bool conn = myhdfs.connect();
	if(!conn)
		cout<<"Reducer: Unable to connect :(\n";
	else
		cout<<"Reducer: Able to connect :) \n";
	stringstream ss1;
	ss1 << my_partition;
	string file_location = myoutput.path + "final" + ss1.str();
	cout<<"Reducer: The output file is "<<file_location<<endl;
	myhdfs.createFile(file_location);
	if(myhdfs.checkExistence(file_location))
		cout<<"Reducer: file in location!!\n";
	Aggregator::iterator aggiter;
	Aggregator* temp = (my_reducer->aggreg);
	for(aggiter = temp->begin(); aggiter != temp->end(); ++aggiter)
	{	
		string k = aggiter->first;
		PartialAgg* curr_par = aggiter->second;
		string val = curr_par->value;
		string out = Write(k, val);
		cout<<"Reducer: The value that it writes is "<<out<<endl;
		myhdfs.writeToFile(file_location,out.c_str(),out.size());
	}
	myhdfs.closeFile(file_location);
	bool disconn = 	myhdfs.disconnect();
	if(!disconn)
		cout<<"Reducer: Not able to disconnect"<<endl;
	else
		cout<<"Reducer: Able to disconnect"<<endl;
	taskreg->setStatus(jobid, jobstatus::DONE);
	return NULL;
}
 

	


