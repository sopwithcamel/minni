#ifndef MINNIE_MASTER_NODE_H
#define MINNIE_MASTER_NODE_H

#include <string>
#include <map>
#include "common.h"
#include "Communicator.h"
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"

using namespace std;
using namespace workdaemon;

class Node
{
	public:
		Node(string* URL) : communicator(URL) {};
		~Node();
		void addMap(JobID jid, ChunkID cid, string fileIn);
		void addReduce(JobID jid, PartID pid, string fileOut);
		bool removeMap(JobID jid);
		bool removeReduce(JobID jid);
		void setMapStatus(JobID jid, Status stat);
		void setReduceStatus(JobID jid, Status stat);
		void checkStatus(map<JobID, Status> & _return);
		void reportCompletedJobs(const std::vector<string> & done);
		string* getURL();

	private:
		JobID remainingMaps;
		JobID remainingReduces;
		map<JobID, struct MapJob*> maps;
		map<JobID, struct ReduceJob*> reduces;
		Communicator communicator;
};

#endif
