/*
  WorkDaemon prototype for 15-712 OS project
  Erik Zawadzki February 22, 2010
*/
#include <iostream>
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"
#include "WorkDaemon_file.h"

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
  TaskRegistry task_reg;
  LocalFileRegistry file_reg;

  empty_task * root;

public:
  WorkDaemonHandler(): task_reg(), file_reg() {
    // Set up the root and a MasterTask.
    root = new(task::allocate_root()) empty_task();
    //MasterTask& t = *new(root->allocate_additional_child_of(*root)) MasterTask(&status_map, &mapper_map, &reducer_map);
    //root->spawn(t); // NB: root hasn't be spawned since that is synchronous
  }
	
  void bark(const string& s) {
    //Your implementation goes here
    //file_reg.recordComplete(1,1,"moose");
    //file_reg.recordComplete(2,1, "badger");
    Location l = {"localhost", 9092};
    Transfer t(1, l);
    t.checkStatus();
    printf("Ruff Ruff: %s\n", s.c_str());
  }

  // Scans the status map and sees if there is anything new to report to the master
  void listStatus(map<JobID, Status> & _return) {
    
  }
	
  void startMapper(const JobID jid, const ChunkID cid) {
	
    // 1) Allocate the mapper task
    MapperTask& t = *new(root->allocate_additional_child_of(*root)) 
      MapperTask(jid,cid,&task_reg);

    // 2) Add to registry
    task_reg.addJob(jid, &t, jobkind::MAPPER);

    // 3) Spawn
    root->spawn(t);
		
  }
	
  void startReducer(const JobID jid, const PartID pid, const string& outFile) {
        // 1) Allocate the mapper task
    ReducerTask& t = *new(root->allocate_additional_child_of(*root)) 
      ReducerTask(jid,pid, "NULL" ,&task_reg);

    // 2) Add to registry
    task_reg.addJob(jid, &t, jobkind::REDUCER);

    // 3) Get files
    // Scene missing

    // 4) Spawn
    root->spawn(t);
  }
	
  void sendData(string & _return, const PartID pid, const BlockID bid) {
    file_reg.bufferData(_return, pid, bid);
  }

  Status dataStatus(const PartID pid, const BlockID sid) {

  }

  Count blockCount(const PartID pid){
    cout << "Getting block counts..." << endl;
    return file_reg.blocks(pid);
  }

	
  void kill(const JobID jid) {    

  }
	
};

int main(int argc, char **argv) {
  int port = 9090;
  if(argc == 2){
    port = atoi(argv[1]);
  }
  cout << "Port: " << port << endl;
  shared_ptr<WorkDaemonHandler> handler(new WorkDaemonHandler());
  shared_ptr<TProcessor> processor(new WorkDaemonProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	
  TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

