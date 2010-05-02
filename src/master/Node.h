#ifndef MINNIE_MASTER_NODE_H
#define MINNIE_MASTER_NODE_H

#include <string>
#include "Communicator.h"
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"

using namespace std;
using namespace workdaemon;

struct MapJob
{
	JobID jid;
	ChunkID cid;
	Status stat;
	string fileIn;
	MapJob (JobID jid, ChunkID cid, Status stat, string fileIn) : jid(jid), cid(cid), stat(stat), fileIn(fileIn) {}
};

struct ReduceJob
{
	JobID jid;
	PartID pid;
	Status stat;
	string fileOut;
	ReduceJob (JobID jid, PartID pid, Status stat, string fileOut) : jid(jid), pid(pid), stat(stat), fileOut(fileOut) {}
};

class Node
{
	private:
		string URL;
		JobID remainingMaps;
		JobID remainingReduces;
		map<JobID, struct MapJob*> maps;
		map<JobID, struct ReduceJob*> reduces;
		Communicator communicator;
	public:
		Node(string URL): URL(URL) {};
		~Node();
		string getURL();
		void addMap(JobID jid, ChunkID cid, string fileIn);
		void addReduce(JobID jid, PartID pid, string fileOut);
		void removeMap(JobID jid);
		void removeReduce(JobID jid);
		void setMapStatus(JobID jid, Status stat);
		void setReduceStatus(JobID jid, Status stat);
};

#endif
