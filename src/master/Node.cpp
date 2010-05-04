#include "Node.h"

Node::~Node()
{

}

void Node::addMap(struct MapJob map)
{
	remainingMaps.push(map);
	remainingMapsCount++;
}

void Node::addReduce(struct ReduceJob reduce)
{
	remainingReduces.push(reduce);
	remainingReducesCount++;
}

/* return true when all maps over */
bool Node::removeMap(JobID jid)
{
	activeMaps.erase(jid);
	activeMapsCount--;
	if (remainingMapsCount == 0) return true;
	return false;
}

/* return true when all reduces over */
bool Node::removeReduce(JobID jid)
{
	activeReduces.erase(jid);
	activeReducesCount--;
	if (remainingReducesCount == 0) return true;
	return false;
}

void Node::runMap()
{
	communicator.sendMap(remainingMaps.top());
	activeMaps[remainingMaps.top().jid] = remainingMaps.top();
	remainingMaps.pop();
	activeMapsCount++;
	remainingMapsCount--;
}

void Node::runReduce()
{
	communicator.sendReduce(remainingReduces.top());
	activeMaps[remainingReduces.top().jid] = remainingReduces.top();
	remainingReduces.pop();
	activeReducesCount++;
	remainingReducesCount--;
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

void Node::sendAllMapsDone()
{
	communicator.sendAllMapsDone();
}

void Node::reportCompletedJobs(const std::vector<string> & done)
{
	communicator.sendReportCompletedJobs(done);
}

bool Node::hasMaps()
{
	if (remainingMapsCount > 0) return true;
	return false;
}

bool hasReduces()
{
	if (remainingReducesCount > 0) return true;
	return false;
}

string* Node::getURL()
{
	return communicator.getURL();
}

JobID Node::numRemainingMapJobs()
{
	return remainingMapsCount;
}

JobID Node::numRemainingReduceJobs()
{
	return remainingReducesCount;
}	

JobID Node::numRemainingJobs()
{
	return remainingMapsCount + remainingReducesCount;
}
