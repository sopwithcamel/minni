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
  boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9091));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);
  transport->open();
  client.bark("request1");
  transport->close();

  
  Location l = {"localhost", 9091};
  Transfer t(1, l, "snake");
  t.checkStatus();
  cout << t.toString() << endl;
  t.getFile();
  t.checkStatus();
  cout << t.toString() << endl;


  transport->open();
  client.bark("request2");
  transport->close();

  t.checkStatus();
  cout << t.toString() << endl;
  t.getFile();
  t.checkStatus();
  cout << t.toString() << endl;
	
  return 0;
}
