#include "Reducer.h"

//ReduceOutput class
int ReduceOutput::Write(string result_key, string result_value) {
	//TODO DFS code to write to the HDFS location specified
}


//Reducer class
void Reducer::AddKeyVal(string key, string value) {
	Aggregator::iterator found = (aggreg)->find(key);
	if(found == (aggreg)->end()) {
		(*aggreg)[key] = new PartialAgg(key,value);
	}
	//Existing key value - then add to the partial result
	else {
		(*aggreg)[key]->add(value);
	}
}

void Reducer::AddPartialAgg(string key, PartialAgg* pagg) {
	Aggregator::iterator found = (aggreg)->find(key);
	if(found == (aggreg)->end()) {
		(*aggreg)[key] = new PartialAgg(key,pagg->get_value());
	}
	//Existing key value - then add to the partial result
	else {
		(*aggreg)[key]->merge(pagg);
	}
}



//Reducer Wrapper
ReduceWrapperTask::ReducerWrapperTask (JobID jid, Properties * p, TaskRegistry * t, GrabberRegistry * g) {
	jobid = jid;
	prop = p;
	taskreg = t;
	grabreg = g
}

int ReducerWrapperTask::ParseProperties(string& soname) {//TODO checking and printing error reports!	
	stringstream ss;
	soname = (*prop)["SO_NAME"];
  	myoutput.path = (*prop)["FILE_IN"];
	string chunk_temp = (*prop)["PID"];
	ss(chunk_temp);
	ss >> my_partition;
	myoutput.master_name = (*prop)["DFS_MASTER"];
        string port_temp = (*prop)["DFS_PORT"];
        ss(port_temp);
	int port_int;
	ss >> port_int;
	myoutput.port = (uint16_t) port_int;
	return 1;
}

int ReducerWrapperTask::UserMapLinking(string soname) {
	return 1;
}

string ReducerWrapperTask::GetCurrentPath() {
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

string ReducerWrapperTask::GetLocalFilename(string path, JobID jobid) {
	stringstream ss;
       	ss << path;
        ss << "/";
       	ss << jobid;
       	return ss.str();
}


task* ReducerWrapperTask::execute() {
	string soname;
	if(ParseProperties(soname,npart) == 1)  {//TODO checking the error cases!
	//TODO exit(); find out about this	
	}
	//dynamically loading the classes
	if(UserMapLinking(soname) == 1) {
		//exit(); TODO find about this!
	}
	Reducer* my_reducer = new Reducer();
	my_reducer->aggreg = new Aggregator();
	
	
	return NULL;
}
 

	


