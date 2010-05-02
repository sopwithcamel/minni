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
  boost::shared_ptr<TSocket> socket1(new TSocket("localhost", 9091));
  boost::shared_ptr<TTransport> transport1(new TBufferedTransport(socket1));
  boost::shared_ptr<TProtocol> protocol1(new TBinaryProtocol(transport1));
	
  string state;

  // The first server has done some work
  cout << "First server:" << endl;
  workdaemon::WorkDaemonClient client1(protocol1);
  transport1->open();
  client1.bark("request1");
  client1.stateString(state); 
  cout << state << endl;
  transport1->close();

  // Tell the client about both servers
  PartitionGrabber pg(1, "snake");
  Location l1 = {"localhost", 9091};
  Location l2 = {"localhost", 9092};
  pg.addLocation(l1);
  pg.addLocation(l2);

  // Get the first server's data
  pg.getMore();

  cout << "Grabber: " << pg.toString() << endl;

  boost::shared_ptr<TSocket> socket2(new TSocket("localhost", 9092));
  boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket2));
  boost::shared_ptr<TProtocol> protocol2(new TBinaryProtocol(transport2));

  // The second server has done some work
  cout << "Second server:" << endl;
  workdaemon::WorkDaemonClient client2(protocol2);
  transport2->open();
  client2.bark("request2");  
  client2.stateString(state); 
  cout << state << endl;

  // Get the second server's data
  pg.getMore();
  pg.getMore();
  cout << "Grabber: " << pg.toString() << endl;

  transport1->close();
  transport2->close();
	
  return 0;
}
