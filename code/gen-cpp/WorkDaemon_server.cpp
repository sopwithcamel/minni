/*
  WorkDaemon prototype for 15-712 OS project
  Erik Zawadzki February 22, 2010
*/
#include <iostream>
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"
#include "WorkDaemon_other.h""

// Thrift includes
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
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
  empty_task * root;
public:
  WorkDaemonHandler() {
    // Set up the root and a MasterTask.
    root = new(task::allocate_root()) empty_task();
    MasterTask& t = *new(root->allocate_additional_child_of(*root)) MasterTask(&status_map, &mapper_map);
    root->spawn(t); // NB: root hasn't be spawned since that is synchronous
  }
	
  void bark(const string& s) {
    //Your implementation goes here
     this_tbb_thread::sleep(tick_count::interval_t((double)5));
    printf("%s\n", s.c_str());
  }
	
  void pulse(map<JobID, Status> & _return) {
    // Your implementation goes here
    printf("pulse\n");
  }
	
  void startMapper(const JobID jid, const ChunkID cid) {
    JobStatusMap::accessor status_accessor;
    JobMapperMap::accessor mapper_accessor;
    // 1) Set the initial status of the job to inprogress
    status_map.insert(status_accessor, jid);
    status_accessor->second = TASK_INPROGRESS;	
		
    // 2) Allocate the mapper task
    MapperTask& t = *new(root->allocate_additional_child_of(*root)) 
      MapperTask(jid,cid,&status_map);

    // 3) Add the allocator task to the mapper map
    mapper_map.insert(mapper_accessor,jid);
    mapper_accessor->second = &t;

    // 4) Spawn
    root->spawn(t);
		
  }
	
  void startReducer(const JobID jid, const KeyID kid, const string& outFile) {
    // Your implementation goes here
    printf("startReducer\n");
  }
	
  void sendData(vector<string> & _return, const JobID jid, const KeyID kid, const SeriesID sid) {
    // Your implementation goes here
    printf("sendData\n");
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
    status_accessor->second = TASK_DEAD;
  }
	
};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<WorkDaemonHandler> handler(new WorkDaemonHandler());
  shared_ptr<TProcessor> processor(new WorkDaemonProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	
  TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

