#ifndef MINNIE_MASTER_COMMUNICATOR_H
#define MINNIE_MASTER_COMMUNICATOR_H

#include "WorkDaemon.h"
#include "Node.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace workdaemon;

/* Thrift abstraction layer */
class Communicator
{
	private:
		
	public:
		Communicator() {};
		~Communicator() {};
		void sendMap(string URL, MapJob map);
		void sendReduce(string URL, ReduceJob reduce);
		void send
};

#endif 
