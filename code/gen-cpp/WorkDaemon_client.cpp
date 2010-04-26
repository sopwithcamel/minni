#include "WorkDaemon.h"
#include <iostream>

#include "tbb/tbb_thread.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace std;
using namespace workdaemon;
using namespace tbb;

template<class U, class T> void print_map(map<U,T> &m){
  cout << "[";
  for(typename map<U, T>::iterator it = m.begin(); it != m.end(); it++){
    cout << it->first << " -> " << it->second << endl; 
  }
  cout << "]" << endl;
}

int main(int argc, char **argv) {
  boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9090));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);
  transport->open();
  client.startMapper(1,1);
  client.startMapper(2,2);
  client.startMapper(3,3);
  
  this_tbb_thread::sleep(tick_count::interval_t((double)3));
  map<JobID, Status> status_map;
  client.listStatus(status_map);
  print_map(status_map);
  this_tbb_thread::sleep(tick_count::interval_t((double)1));
  client.listStatus(status_map);
  print_map(status_map);
  transport->close();
	
  return 0;
}
