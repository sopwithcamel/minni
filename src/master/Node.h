#ifndef MINNIE_MASTER_NODE_H
#define MINNIE_MASTER_NODE_H

#include <string>
#include <map>
#include <queue>
#include "common.h"
#include "Communicator.h"
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"
#include "Minnie.h"

using namespace std;
using namespace workdaemon;

class Node
{
	public:
		Node(string* URL, MapReduceSpecification spec);
		~Node();
		void addMap(struct MapJob);
		void addReduce(struct ReduceJob);
		bool removeMap(JobID jid);
		bool removeReduce(JobID jid);
		bool runMap();
		bool runReduce();
		void setMapStatus(JobID jid, Status stat);
		void setReduceStatus(JobID jid, Status stat);
		void checkStatus(map<JobID, Status> & _return);
		void reportCompletedJobs(const std::vector<string> & done);
		void sendAllMapsFinished();
		bool hasMaps();
		bool hasReduces();
		string* getURL();
		JobID numRemainingMapJobs();
		JobID numRemainingReduceJobs();		
		JobID numRemainingJobs();
		JobID numActiveJobs();
		struct MapJob stealMap();
		struct ReduceJob stealReduce();

	private:
		MapReduceSpecification spec;
		JobID remainingMapsCount;
		JobID remainingReducesCount;
		JobID activeMapsCount;
		JobID activeReducesCount;
		map<JobID, struct MapJob> activeMaps;
		map<JobID, struct ReduceJob> activeReduces;
		queue<struct MapJob> remainingMaps;
		queue<struct ReduceJob> remainingReduces;
		Communicator communicator;
};

#endif
