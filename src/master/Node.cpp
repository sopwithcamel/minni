#include "Node.h"

Node::~Node()
{

}

void Node::addMap(JobID jid, ChunkID cid, string fileIn)
{
	struct MapJob* map = new struct MapJob(jid, cid, jobstatus::INPROGRESS, fileIn);
	maps[map->jid] = map;
	communicator.sendMap(*map);
	remainingMaps++;
}

void Node::addReduce(JobID jid, PartID pid, string fileOut)
{
	struct ReduceJob* reduce = new struct ReduceJob(jid, pid, jobstatus::INPROGRESS, fileOut);
	reduces[reduce->jid] = reduce;
	communicator.sendReduce(*reduce);
	remainingReduces++;
}

/* return true when all maps over */
bool Node::removeMap(JobID jid)
{
	maps.erase(jid);
	remainingMaps--;
	if (remainingMaps == 0) return true;
	return false;
}

/* return true when all reduces over */
bool Node::removeReduce(JobID jid)
{
	reduces.erase(jid);
	remainingReduces--;
	if (remainingReduces == 0) return true;
	return false;
}

void Node::setMapStatus(JobID jid, Status stat)
{
	maps[jid]->stat = stat;
}

void Node::setReduceStatus(JobID jid, Status stat)
{
	reduces[jid]->stat = stat;
}

void Node::checkStatus(std::map<JobID, Status> & _return)
{
	communicator.sendListStatus(_return);
}

void Node::reportCompletedJobs(const std::vector<string> & done)
{
	communicator.sendReportCompletedJobs(done);
}

string* Node::getURL()
{
	return communicator.getURL();
}
