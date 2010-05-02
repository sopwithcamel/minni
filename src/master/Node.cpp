#include "Node.h"
#include "Communicator.h"

Node::~Node()
{
	
}

string Node::getURL()
{
	return URL;
}

void Node::addMap(JobID jid, ChunkID cid, string fileIn)
{
	struct MapJob* map = new struct MapJob(jid, cid, jobstatus::INPROGRESS, fileIn);
	maps[map->jid] = map;
	remainingMaps++;
}

void Node::addReduce(JobID jid, PartID pid, string fileOut)
{
	struct ReduceJob* reduce = new struct ReduceJob(jid, pid, jobstatus::INPROGRESS, fileOut);
	reduces[reduce->jid] = reduce;
	remainingReduces++;
}

void Node::removeMap(JobID jid)
{
	maps.erase(jid);
}

void Node::removeReduce(JobID jid)
{
	reduces.erase(jid);
}

void Node::setMapStatus(JobID jid, Status stat)
{
	maps[jid]->stat = stat;
}

void Node::setReduceStatus(JobID jid, Status stat)
{
	reduces[jid]->stat = stat;
}
