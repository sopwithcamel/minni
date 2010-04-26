/*
  WorkDaemon prototype for 15-712 OS project
  Erik Zawadzki February 22, 2010
*/
#include <iostream>
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"
#include "WorkDaemon_other.h"

// Thrift includes
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/blocked_range.h"
#include "tbb/tbb_thread.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace workdaemon;
using namespace tbb;
using namespace std;

// Server stubs

class WorkDaemonHandler : virtual public WorkDaemonIf {
  JobStatusMap status_map;
  JobMapperMap mapper_map;
  JobReducerMap reducer_map;

  FileRegistry file_reg;
  empty_task * root;

public:
  WorkDaemonHandler(): status_map(), mapper_map(),reducer_map(), file_reg(&status_map) {
    // Set up the root and a MasterTask.
    root = new(task::allocate_root()) empty_task();
    MasterTask& t = *new(root->allocate_additional_child_of(*root)) MasterTask(&status_map, &mapper_map, &reducer_map);
    root->spawn(t); // NB: root hasn't be spawned since that is synchronous
  }
	
  void bark(const string& s) {
    //this_tbb_thread::sleep(tick_count::interval_t((double)5));
    printf("%s\n", s.c_str());
  }

  // Scans the status map and sees if there is anything new to report to the master
  void listStatus(map<JobID, Status> & _return) {
    _return.clear(); 

    // Flip through the jobs and see if there are any new status
    JobStatusMap::range_type range = status_map.range();
    for(JobStatusMap::iterator it = range.begin(); it !=range.end(); it++){
      if(it->second == JobStatus::DONE){
	_return[it->first] = JobStatus::DONE;
	it->second == JobStatus::DONE_AND_REPORTED;
      }
      if(it->second == JobStatus::DEAD){
	_return[it->first] = JobStatus::DEAD;
	it->second == JobStatus::DEAD_AND_REPORTED;
      }
    }
  }
	
  void startMapper(const JobID jid, const ChunkID cid) {
    JobStatusMap::accessor status_accessor;
    JobMapperMap::accessor mapper_accessor;
    // 1) Set the initial status of the job to inprogress
    status_map.insert(status_accessor, jid);
    status_accessor->second = JobStatus::INPROGRESS;	
		
    // 2) Allocate the mapper task
    MapperTask& t = *new(root->allocate_additional_child_of(*root)) 
      MapperTask(jid,cid,&status_map);

    // 3) Add the allocator task to the mapper map
    mapper_map.insert(mapper_accessor,jid);
    mapper_accessor->second = &t;

    // 4) Spawn
    root->spawn(t);
		
  }
	
  void startReducer(const JobID jid, const PartitionID pid, const string& outFile) {
    JobStatusMap::accessor status_accessor;
    JobReducerMap::accessor reducer_accessor;
    // 1) Set the initial status of the job to inprogress
    status_map.insert(status_accessor, jid);
    status_accessor->second = JobStatus::INPROGRESS;	
		
    // 2) Allocate the mapper task
    ReducerTask& t = *new(root->allocate_additional_child_of(*root)) 
      ReducerTask(jid,pid,outFile,&status_map);

    // 3) Add the allocator task to the mapper map
    reducer_map.insert(reducer_accessor,jid);
    reducer_accessor->second = &t;

    // 4) Spawn
    root->spawn(t);
  }
	
  void sendData(std::vector<std::vector<std::string> > & _return, const PartitionID pid, const SeriesID sid) {
    // Your implementation goes here
    printf("sendData\n");
    file_reg.find_new(0);
    file_reg.find_new(1);
    cout << file_reg.to_string();
  }

  Status dataStatus(const PartitionID pid) {
    printf("dataStatus\n");
    file_reg.find_new(pid);
    cout << file_reg.to_string();
  }

	
  void kill(const JobID jid) {    
    // 1) Find the right job
    JobMapperMap::accessor mapper_accessor;
    JobStatusMap::accessor status_accessor;
    bool found = mapper_map.find(mapper_accessor, jid);

    // 2) Kill it (NB: doesn't actually cancel...)
    root->destroy(*mapper_accessor->second);
    if(!found){
      cerr << "Asked to kill "
	   << jid 
	   << " but it doesn't exist..."
	   << endl;
    }

    // 3) Mark it as dead
    status_map.insert(status_accessor, jid);
    status_accessor->second = JobStatus::DEAD;
  }
	
};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<WorkDaemonHandler> handler(new WorkDaemonHandler());
  shared_ptr<TProcessor> processor(new WorkDaemonProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	
  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

