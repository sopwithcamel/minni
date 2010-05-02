#ifndef MINNIE_MASTER_COMMON_H
#define MINNIE_MASTER_COMMON_H

#include <string>
#include <iostream>
#include "WorkDaemon.h"
#include "daemon_types.h"

using namespace workdaemon;
using namespace std;

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

#endif
