// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.
#include "common.h"
#include "WorkDaemon.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace workdaemon;

class WorkDaemonHandler : virtual public WorkDaemonIf {
 public:
  WorkDaemonHandler() {
    // Your initialization goes here
  }

  void bark(const std::string& s) {
    // Your implementation goes here
    printf("bark\n");
  }

  void stateString(std::string& _return) {
    // Your implementation goes here
    printf("stateString\n");
  }

  void kill() {
    // Your implementation goes here
    printf("kill\n");
  }

  void listStatus(std::map<JobID, Status> & _return) {
    // Your implementation goes here
    printf("listStatus\n");
  }

  void startMapper(const JobID jid, const Properties& prop) {
    // Your implementation goes here
    printf("startMapper\n");
  }

  void startReducer(const JobID jid, const Properties& prop) {
    // Your implementation goes here
    printf("startReducer\n");
  }

  void sendData(std::string& _return, const PartID kid, const BlockID bid) {
    // Your implementation goes here
    printf("sendData\n");
  }

  Status mapperStatus() {
    // Your implementation goes here
    printf("mapperStatus\n");
  }

  Count blockCount(const PartID pid) {
    // Your implementation goes here
    printf("blockCount\n");
  }

  void reportCompletedJobs(const std::vector<URL> & done) {
    // Your implementation goes here
    printf("reportCompletedJobs\n");
  }

  void allMapsDone() {
    // Your implementation goes here
    printf("allMapsDone\n");
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

