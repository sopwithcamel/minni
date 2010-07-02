#include "config.h"
#include "Reducer.h"

//ReduceOutput class


//Reducer class
void Reducer::AddKeyVal(string key, string value) {
	Aggregator::iterator found = (aggreg)->find(key);
	if(found == (aggreg)->end()) {
		(*aggreg)[key] = new PartialAgg(value); /*deleted at the end*/
	}
	//Existing key value - then add to the partial result
	else {
		(*aggreg)[key]->add(value);
	}
}

void Reducer::AddPartialAgg(string key, PartialAgg* pagg) {
	cout<<"addin a pagg whose value is "<<pagg->get_value()<<endl;
	Aggregator::iterator found = (aggreg)->find(key);
	if(found == (aggreg)->end()) {
		cout<<"New key "<<key<<" found and adding a new pagg into aggregator\n";
		(*aggreg)[key] = new PartialAgg(pagg->get_value()); /*deleted at the end*/
		cout<<"The new aggregator addition happened successfully\n";
	}
	//Existing key value - then add to the partial result
	else {
		cout<<"Existing key "<<key<<"found and adding to partial reuslt"<<endl;
		(*aggreg)[key]->merge(pagg);
		cout<<"Added the existing key to aggreg\n";
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

int deSerialize(FILE* fileIn, char* type, uint64_t* keyLength, char** key, uint64_t* valueLength, char** value)
{
	size_t result;
	if (feof(fileIn)) return -1;
        result = fread(type, sizeof(char), 1, fileIn);
	if(result != 1) {cout<<"Reading error: TYPE "<<result<<endl; return -1;}
	if (feof(fileIn)) return -1;
        result = fread(keyLength, sizeof(uint64_t), 1, fileIn);
        if(result != 1) {cout<<"Reading error: KEY LEN " <<result<<endl; return -1;}
	if (feof(fileIn)) return -1;
        *key = (char*) malloc(*keyLength); /*freed line 136 after the particular loop run*/
	if (feof(fileIn)) return -1;
        result = fread(*key, sizeof(char), *keyLength, fileIn);
        if(result != *keyLength) {cout<<"Reading error: KEY VALUE "<<result<<endl; return -1;}
	if (feof(fileIn)) return -1;
        result = fread(valueLength, sizeof(uint64_t), 1, fileIn);
        if(result != 1) {cout<<"Reading error: VALUE LEN"<<result<<endl; return -1;}

	if (feof(fileIn)) return -1;
        *value = (char*) malloc(*valueLength); /*freed line 137 after the particular loop run*/
	if (feof(fileIn)) return -1;
        result = fread(*value, sizeof(char), *valueLength, fileIn);
        if(result != *valueLength){cout<<"Reading error: VALUE VALUE "<<result<<endl; return -1;}
	cout<<"Reducer: The values are key= "<<*key<<" value= "<<*value<<endl;
	return 0;
}

void ReducerWrapperTask::DoReduce(string filename) {
	cout<<"I am here to do reduce on "<<filename<<endl;
	FILE* fptr = fopen(filename.c_str(),"rb");
	char type;
	string key1, value1;
	//adding values
	while(feof(fptr)==0)//not end of file //TODO check
	{
		cout<<"Inside and now going to read the file "<<endl;
		char *key2, *value2;
		uint64_t keylength, valuelength;
		if (deSerialize(fptr, &type, &keylength, &key2, &valuelength, &value2)) break;
		if(type == 2)
		{
			cout<<"Type is key value and I am adding "<<"("<<key2<<" "<<value2<<"key value pair\n";
			my_reducer->AddKeyVal(key2,value2);
		}
		else if (type == 1)
		{
			cout<<"Type is partial aggregate and i am adding "<<"("<<key2<<" "<<value2<<"partial aggregator\n";
			PartialAgg* newpagg = new PartialAgg(value2); /*deleted after adding value*/
			my_reducer->AddPartialAgg(key2, newpagg);
			delete newpagg;
			cout<<"Added the partial aggregate\n";
		}
		free(key2);
		free(value2);
	}
	fclose(fptr);
}

string ReducerWrapperTask::Write(string result_key, string result_value) {
	string out = result_key + " " + result_value + "\n";
	return out;
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
	my_reducer = new Reducer(); /*deleted at the end*/
	my_reducer->aggreg = new Aggregator(); /*deleted at the end*/
		
	//filename for the results
	string curr_path = GetCurrentPath();
	string filename = GetLocalFilename(curr_path,jobid);

	int sleeptime = BASE_SLEEPTIME;
	grabreg->setupGrabber(my_partition); //setting up the grabber
	PartStatus curr_stat = grabreg->getStatus(my_partition);
	int flag = 0;
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
			flag = 1;
			cout<<"Reducer: Reached Ready state!! \n";
			grabreg->getMore(my_partition, filename);
			cout<<"Reducer: Going to do reduce on the file "<<filename<<endl;
			DoReduce(filename);
			cout<<"Reducer: Done with reducing \n";
			sleeptime = BASE_SLEEPTIME;
		}
		
		curr_stat = grabreg->getStatus(my_partition);
		
	}
	if(flag == 0)
		cout<<"Reducer: Never entered ready state before coming here! How does that happen?\n";
	else
		cout<<"Reducer: This is normal! I saw a ready before coming here\n";
	cout<<"Reducer: The master is "<<myoutput.master_name<<endl;
	cout<<"Reducer: The output port is "<<myoutput.port<<endl;

	KDFS myhdfs(myoutput.master_name,myoutput.port);
	
	cout<<"Reducer: Opening the KDFS\n";
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
	else
		cout<<"Reducer: no file available\n";
	Aggregator::iterator aggiter;
	Aggregator* temp = (my_reducer->aggreg);
	cout<<"Reducer: goin to read from the aggregators\n";
	for(aggiter = temp->begin(); aggiter != temp->end(); ++aggiter)
	{	
		cout<<"Reducer: Inside reading from the aggregators\n";
		string k = aggiter->first;
		PartialAgg* curr_par = aggiter->second;
		string val = curr_par->value;
		cout << "Key: " << k << " " << " val: " << val << endl;
		string out = Write(k, val);
		cout<<"Reducer: The value that it writes is "<<out<<endl;
		myhdfs.writeToFile(file_location,out.c_str(),out.size());
	}
	cout<<"Reducer: Outside the loop\n";
	myhdfs.closeFile(file_location);
	bool disconn = 	myhdfs.disconnect();
	if(!disconn)
		cout<<"Reducer: Not able to disconnect"<<endl;
	else
		cout<<"Reducer: Able to disconnect"<<endl;
	taskreg->setStatus(jobid, jobstatus::DONE);
	
	//deleting things
	for(aggiter = temp->begin(); aggiter != temp->end(); ++aggiter)
	{	
		delete (aggiter->second);
	}

	delete my_reducer->aggreg;
	delete my_reducer;
	

	return NULL;
}
 

	


