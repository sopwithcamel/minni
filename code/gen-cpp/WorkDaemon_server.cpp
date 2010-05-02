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
  GrabberMap grab_map;

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
    if(s.compare("request1") == 0){
      file_reg.recordComplete(1,1,"moose");
    }
    if(s.compare("request2") == 0){
      file_reg.recordComplete(2,1, "badger");
    }

    printf("Finished: %s\n", s.c_str());
  }

  void stateString(string &_return){
    stringstream ss;
    ss << "Task Reg: " << task_reg.toString() << endl;
    ss << "File Reg: " <<  file_reg.toString() << endl;
    _return = ss.str();
    return;
  }

  // Scans the status map and sees if there is anything new to report to the master
  // Will either be jobstatus::DONE or jobstatus::DEAD
  void listStatus(map<JobID, Status> & _return) {
    task_reg.getReport(_return);
    return;
  }
	
  void startMapper(const JobID jid, const string& infile, const ChunkID cid) {
    assert(!task_reg.exists(jid));
    // 1) Allocate the mapper task
    MapperTask& t = *new(root->allocate_additional_child_of(*root)) 
      MapperTask(jid,cid,&task_reg);

    // 2) Add to registry
    task_reg.addJob(jid, &t, jobkind::MAPPER);

    // 3) Spawn
    root->spawn(t);
		
  }
	
  void startReducer(const JobID jid, const PartID pid, const string& outFile) {
    assert(!task_reg.exists(jid));
    // 1) Allocate the mapper task
    ReducerTask& t = *new(root->allocate_additional_child_of(*root)) 
      ReducerTask(jid,pid, "NULL" ,&task_reg);

    // 2) Add to registry
    task_reg.addJob(jid, &t, jobkind::REDUCER);


    // 3) Get files
    //Scene missing
    GrabberMap::accessor acc_grab;
    grab_map.insert(acc_grab,pid);
    acc_grab->second = PartitionGrabber(pid, "Somefile_" + pid);

    // 4) Spawn
    root->spawn(t);
  }
	
  // RPC function for sending a small chunk of data.
  // FileRegistry deals with the sender side, PartitionGrabber deals with
  // the reciever side
  void sendData(string & _return, const PartID pid, const BlockID bid) {
    file_reg.bufferData(_return, pid, bid);
  }

  // Are we still waiting on some mappers?
  Status partitionStatus(const PartID pid) {
    if(true || task_reg.mapper_still_running()){
      return jobstatus::INPROGRESS;
    }
    return jobstatus::DONE;
  }

  // How many blocks do we know about?
  Count blockCount(const PartID pid){
    return file_reg.blocks(pid);
  }

  void reportCompletedJobs(const vector<URL> & done){
    GrabberMap::range_type range = grab_map.range();
    for(GrabberMap::iterator it = range.begin();
	it != range.end(); it++){
      it->second.addLocations(done);
    }
  }

  void kill(){
    // Crash the node.
    exit(-1);
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

