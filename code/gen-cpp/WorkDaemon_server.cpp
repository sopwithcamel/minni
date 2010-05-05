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
  GrabberRegistry grab_reg;

  unsigned int id;

  empty_task * root;

public:
  WorkDaemonHandler(): task_reg(), file_reg() {
    // Set up the root and a MasterTask.
    root = new(task::allocate_root()) empty_task();
    //MasterTask& t = *new(root->allocate_additional_child_of(*root)) MasterTask(&status_map, &mapper_map, &reducer_map);
    //root->spawn(t); // NB: root hasn't be spawned since that is synchronous
  }

  // Swiss-army knife debug function
  void bark(const string& s) {
    cout << id++ << ": Barking..." << endl;
    if(s.compare("request1") == 0){
      file_reg.recordComplete(1,1,"moose");
    }
    if(s.compare("request2") == 0){
      file_reg.recordComplete(2,1, "badger");
    }
    if(s.compare("request3") == 0){
      file_reg.recordComplete(2,1, "pika");
    }
    cout << "Done" << endl;

  }

  // Useful for querying the state of a remote object
  void stateString(string &_return){
    cout << id++ << ": Returning state string..." << endl;
    stringstream ss;
    ss << "Task Reg: " << task_reg.toString() << endl;
    ss << "File Reg: " <<  file_reg.toString() << endl;
    ss << "Grabber: " << grab_reg.toString() << endl;
    _return = ss.str();
    cout << "Done" << endl;
    return;
  }

  // Scans the status map and sees if there is anything new to report to the master
  // Will either be jobstatus::DONE or jobstatus::DEAD
  void listStatus(map<JobID, Status> & _return) {
    cout << id++ << ": Listing Status..." << endl;
    task_reg.getReport(_return);
    //task_reg.cullReported();
    cout << "Done" << endl;
    return;
  }
	
  void startMapper(const JobID jid, const Properties& prop) {
    cout << id++ << ": Starting Mapper " << jid << "..." << endl;
    if(task_reg.exists(jid)){
      cout << "Repeat, skipping." << endl;
      if(task_reg.getStatus(jid) == jobstatus::DEAD_AND_REPORTED){
	task_reg.setStatus(jid, jobstatus::DEAD);
      }
      if(task_reg.getStatus(jid) == jobstatus::DONE_AND_REPORTED){
	task_reg.setStatus(jid, jobstatus::DONE);
      }
      return;
    }
    // 1) Allocate the mapper task
    Properties * p_copy = new Properties(prop); // Deleted by ~Mapper
    MapperTask& t = *new(root->allocate_additional_child_of(*root)) 
      MapperTask(jid, p_copy, &task_reg, &file_reg);

    // 2) Add to registry
    task_reg.addJob(jid, &t, jobkind::MAPPER);

    
    // 3) Spawn
    root->increment_ref_count();
    root->spawn(t);
    cout << "Done" << endl;
  }
	
  void startReducer(const JobID jid, const Properties& prop) {
    cout << id++ << ": Starting Reducer " << jid << "..." << endl;
    if(task_reg.exists(jid)){
      cout << "Repeat, skipping." << endl;
      if(task_reg.getStatus(jid) == jobstatus::DEAD_AND_REPORTED){
	task_reg.setStatus(jid, jobstatus::DEAD);
      }
      if(task_reg.getStatus(jid) == jobstatus::DONE_AND_REPORTED){
	task_reg.setStatus(jid, jobstatus::DONE);
      }
      return;
    }
    //cout << "Assert passed" << endl;
    // 1) Allocate the mapper task
    Properties * p_copy = new Properties(prop); // Deleted by ~Reducer
    ReducerTask& t = *new(root->allocate_additional_child_of(*root)) 
      ReducerTask(jid, p_copy, &task_reg, &grab_reg);
    //cout << "Allocate passed" << endl;

    // 2) Add to registry
    task_reg.addJob(jid, &t, jobkind::REDUCER);
    //cout << "Add passed" << endl;

    // 3) Get files
    //Scene missing
    //GrabberMap::accessor acc_grab;
    //grab_map.insert(acc_grab,pid);
    //acc_grab->second = PartitionGrabber(pid, "Somefile_" + pid);

    // 4) Spawn
    root->increment_ref_count();
    root->spawn(t);
    cout << "Done" << endl;
  }
	
  // RPC function for sending a small chunk of data.
  // FileRegistry deals with the sender side, PartitionGrabber deals with
  // the reciever side
  void sendData(string & _return, const PartID pid, const BlockID bid) {
    cout << id++ << ": Sending data..." << endl;
    file_reg.bufferData(_return, pid, bid);
    cout << "Done" << endl;
  }

  // Are we still waiting on some mappers?
  Status mapperStatus() {
    cout << id++ << ": Mapper status..." << endl;
    if(task_reg.mapper_still_running()){
      return jobstatus::INPROGRESS;
    }
    return jobstatus::DONE;
  }

  // How many blocks do we know about?
  Count blockCount(const PartID pid){
    cout << id++ << ": Counting blocks..." << endl;
    cout << "Done" << endl;
    return file_reg.blocks(pid);
  }

  // Used by the master to report finished mappers
  void reportCompletedJobs(const vector<URL> & done){
    cout << id++ << ": Master reporting work..." << endl;
    
    grab_reg.addLocations(done);

    cout << "Done" << endl;
  }

  // Sounds the all clear when all mappers are done
  void allMapsDone(){
    grab_reg.reportDone();
  }
  
    // Crash the node.
  void kill(){
    cout << id++ << ": Kill..." << endl;
    file_reg.clear();
    grab_reg.clear();
    task_reg.clear();
  }
	
};

int main(int argc, char **argv) {
  int port = 9090;
  if(argc == 2){
    port = atoi(argv[1]);
  }
  cout << "--- Starting Minni ---" << endl;
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

