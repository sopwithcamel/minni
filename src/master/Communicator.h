#ifndef MINNIE_MASTER_COMMUNICATOR_H
#define MINNIE_MASTER_COMMUNICATOR_H

#include <string>
#include "common.h"
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"
#include "daemon_types.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace workdaemon;

using namespace std;

/* Thrift abstraction layer */
class Communicator
{
	public:
		Communicator(string* url) : url(url) {};							/* constructor */
		~Communicator();											/* destructor */
		void sendMap(MapJob map);									/* start a map job */
		void sendReduce(ReduceJob reduce);							/* start a reduce job */
		void sendListStatus(std::map<JobID, Status> & _return);			/* poll for status */
		void sendReportCompletedJobs(const std::vector<URL> & done);	/* send once per node */
		string* getURL();
	private:
		string* url;
};

#endif 
