#include "Reducer.h"
#include "HashAggregator.h"
#include "BucketAggregator.h"

//Reducer class
Reducer::Reducer(PartialAgg* (*__createPAO)(const char* t), 
			void (*__destroyPAO)(PartialAgg* p)):
		createPAO(__createPAO),
		destroyPAO(__destroyPAO)
{
}

Reducer::~Reducer()
{
}

//Reducer Wrapper
ReducerWrapperTask::ReducerWrapperTask (JobID jid, Properties * p, TaskRegistry * t,
			GrabberRegistry * g):
		jobid(jid),
		prop(p),
		taskreg(t),
		grabreg(g)
{
	cfg.readFile(CONFIG_FILE);
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

int ReducerWrapperTask::UserMapLinking(const char* soname) {
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
	__libminni_destroy_pao = (void (*)(PartialAgg*)) dlsym(handle, "__libminni_pao_destroy");
	if ((err = dlerror()) != NULL)
	{
		fprintf(stderr, "Error locating symbol __libminni_create_pao in %s\n", err);
		exit(-1);
	}
	reducer = new Reducer(__libminni_create_pao, __libminni_destroy_pao); /*deleted at the end*/
	
	return SUCCESS_EXIT;
}

string ReducerWrapperTask::Write(string result_key, string result_value) {
	string out = result_key + " " + result_value + "\n";
	return out;
}

task* ReducerWrapperTask::execute() {
	string soname;
	char *s_name;
	char *input_file = (char*)malloc(FILENAME_LENGTH);
	strcpy(input_file, "mapfile");
	cout<<"Going to call Parse properties \n";
	if(ParseProperties(soname) == ERROR_EXIT)  {
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return NULL;
	}
	cout<<"After parse properties\n";
	//dynamically loading the classes
	s_name = (char*)malloc(soname.length() + 1);
	strcpy(s_name, soname.c_str());
	if(UserMapLinking(s_name) == ERROR_EXIT) {
		taskreg->setStatus(jobid, jobstatus::DEAD);
		return NULL;
	}

	Setting& c_sel_aggregator = cfg.lookup("minni.aggregator.selected.reduce");
	string selected_reduce_aggregator = (const char*)c_sel_aggregator;

	if (!selected_reduce_aggregator.compare("simple")) {
		reducer->aggreg = dynamic_cast<Aggregator*>(new HashAggregator(&cfg,
					Reduce, 0, NULL, input_file, reducer->createPAO, 
					reducer->destroyPAO, "result"));
	} else if (!selected_reduce_aggregator.compare("bucket")) {
		reducer->aggreg = dynamic_cast<Aggregator*>(new BucketAggregator(&cfg,
					Reduce, 0, NULL, input_file, reducer->createPAO, 
					reducer->destroyPAO, "result"));
	}
		
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
			// TODO: what does this do?
			grabreg->getMore(my_partition, input_file);
//			cout<<"Reducer: Going to do reduce on the file "<<filename<<endl;
			reducer->aggreg->runPipeline();
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

	delete reducer->aggreg;
	delete reducer;

	return NULL;
}
