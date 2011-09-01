#include "WorkDaemon.h"  // As an example

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace workdaemon;

int main(int argc, char **argv) {
  boost::shared_ptr<TSocket> socket(new TSocket("128.237.226.102", 9090));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

  WorkDaemonClient client(protocol);
  transport->open();
  client.bark("Hello Erik Zawadzki. All your base are belong to us.");
  transport->close();

  return 0;
}
