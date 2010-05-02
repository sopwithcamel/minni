#include "WorkDaemon.h"
#include "WorkDaemon_file.h"
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
  boost::shared_ptr<TSocket> socket1(new TSocket("localhost", WORKER_PORT));
  boost::shared_ptr<TTransport> transport1(new TBufferedTransport(socket1));
  boost::shared_ptr<TProtocol> protocol1(new TBinaryProtocol(transport1));
	
  string state;

  // The first server has done some work
  cout << "First server:" << endl;
  workdaemon::WorkDaemonClient client1(protocol1);
  transport1->open();

  //map<string,string> prop;
  //client1.startMapper(5, prop);
  vector<URL> urls;
  urls.push_back("localhost");
  client1.bark("request1");
  client1.bark("request2");
  client1.stateString(state); 
  client1.reportCompletedJobs(urls);
  client1.stateString(state); 
  cout << state << endl;
  transport1->close();

  GrabberRegistry reg;
  reg.addLocations(urls);
  reg.getMore(1);
  
  cout << reg.toString() << endl;

 
  return 0;
}
