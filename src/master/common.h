#ifndef MINNIE_MASTER_COMMON_H
#define MINNIE_MASTER_COMMON_H

#include <string>
#include <iostream>
#include <sstream>
#include "WorkDaemon.h"
#include "daemon_types.h"

using namespace workdaemon;
using namespace std;

struct MapJob
{
	JobID jid;
	Status stat;
	Properties prop;
	MapJob (JobID jid, ChunkID cid, Status stat, string fileIn) : jid(jid), stat(stat)
	{
		ostringstream stringConverter;
		stringConverter << cid;
		prop["cid"] = stringConverter.str();
		prop["fileIn"] = fileIn;
		prop["SO_NAME"] = "";
		prop["MAPPER"] = "";		
	}
};

struct ReduceJob
{
	JobID jid;
	Status stat;
	Properties prop;
	ReduceJob (JobID jid, PartID pid, Status stat, string fileOut) : jid(jid), stat(stat)
	{
		ostringstream stringConverter;
		stringConverter << pid;
		prop["pid"] = stringConverter.str();
		prop["fileOut"] = fileOut;
		prop["SO_NAME"] = "";
		prop["REDUCER"] = "";
	}
};

#endif
