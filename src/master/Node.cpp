#include "Node.h"

Node::Node(string* URL, MapReduceSpecification spec) : spec(spec), communicator(URL)
{
	remainingMapsCount = 0;
	remainingReducesCount = 0;
	activeMapsCount = 0;
	activeReducesCount = 0;
}

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

bool Node::runMap()
{
	if (remainingMaps.empty()) return false;
	communicator.sendMap(remainingMaps.front());
	activeMaps[remainingMaps.front().jid] = remainingMaps.front();
	remainingMaps.pop();
	activeMapsCount++;
	remainingMapsCount--;
	return true;
}

bool Node::runReduce()
{
	if (remainingReduces.empty()) return false;
	communicator.sendReduce(remainingReduces.front());
	activeReduces[remainingReduces.front().jid] = (remainingReduces.front());
	remainingReduces.pop();
	activeReducesCount++;
	remainingReducesCount--;
	return true;
}

void Node::setMapStatus(JobID jid, Status stat)
{
	activeMaps[jid].stat = stat;
}

void Node::setReduceStatus(JobID jid, Status stat)
{
	activeReduces[jid].stat = stat;
}

void Node::checkStatus(std::map<JobID, Status> & _return)
{
	communicator.sendListStatus(_return);
}

void Node::sendAllMapsFinished()
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

bool Node::hasReduces()
{
	if ( remainingReducesCount > 0) return true;
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

JobID Node::numActiveJobs()
{
	return activeMapsCount + activeReducesCount;
}

struct MapJob Node::stealMap()
{
	MapJob ret = remainingMaps.front();
	remainingMapsCount--;
	remainingMaps.pop();
	return ret;
}

struct ReduceJob Node::stealReduce()
{
	ReduceJob ret = remainingReduces.front();
	remainingReducesCount--;
	remainingReduces.pop();
	return ret;	
}

