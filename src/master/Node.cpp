#include "Node.h"

Node::Node(string* URL) : communicator(URL)
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
	activeMapsCount--;
	if (activeMaps.erase(jid)) return true;
	return false;
}

/* return true when all reduces over */
bool Node::removeReduce(JobID jid)
{
	activeReducesCount--;
	if (activeReduces.erase(jid)) return true;
	return false;
}

bool Node::runMap()
{
	if (remainingMaps.empty()) return false;
	communicator.sendMap(remainingMaps.front(), 3);
	activeMaps[remainingMaps.front().jid] = remainingMaps.front();
	remainingMaps.pop();
	activeMapsCount++;
	remainingMapsCount--;
	return true;
}

bool Node::runReduce()
{
	if (remainingReduces.empty()) return false;
	communicator.sendReduce(remainingReduces.front(), 3);
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
	communicator.sendListStatus(_return, 3);
}

void Node::sendAllMapsFinished()
{
	communicator.sendAllMapsDone(3);
}

void Node::reportCompletedJobs(const std::vector<string> & done)
{
	communicator.sendReportCompletedJobs(done, 3);
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

void Node::sendKill()
{
	communicator.sendKill(3);
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

void Node::printActiveMaps(vector<struct MapJob*> & _return)
{
	map<JobID, struct MapJob>::iterator nodesIter;
	for (nodesIter = activeMaps.begin() ; nodesIter != activeMaps.end(); nodesIter++ )
	{
		cout << "\t\tWaiting for Map[" << ((*nodesIter).second).jid << "] from " << *getURL() << endl;
		_return.push_back(&(((*nodesIter).second)));
	}
}

void Node::printActiveReduces(vector<struct ReduceJob*> & _return)
{
	map<JobID, struct ReduceJob>::iterator nodesIter;
	for (nodesIter = activeReduces.begin() ; nodesIter != activeReduces.end(); nodesIter++ )
	{
		cout << "\t\tWaiting for Reduce[" << ((*nodesIter).second).jid << "] from " << *getURL() << endl;
		_return.push_back(&(((*nodesIter).second)));
	}	
}
