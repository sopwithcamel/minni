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
	uint64_t staleness;
	Properties prop;
	MapJob () {};
	MapJob (JobID jid, ChunkID cid, Status stat, string fileIn, string so_name, JobID numReducers, string dfsMaster, uint16_t dfsPort) : jid(jid), stat(stat)
	{
		ostringstream stringConverter;
		stringConverter << cid;
		prop["CID"] = stringConverter.str();
		stringConverter.str("");
		prop["FILE_IN"] = fileIn;
		prop["SO_NAME"] = so_name;
		prop["NUM_REDUCERS"] = numReducers;
		prop["DFS_MASTER"] = dfsMaster;
		stringConverter << dfsPort;
		prop["DFS_PORT"] = stringConverter.str();
		staleness = 0;
	};
};

struct ReduceJob
{
	JobID jid;
	Status stat;
	uint64_t staleness;
	Properties prop;
	ReduceJob () {};
	ReduceJob (JobID jid, PartID pid, Status stat, string fileOut, string so_name, string dfsMaster, uint16_t dfsPort) : jid(jid), stat(stat)
	{
		ostringstream stringConverter;
		stringConverter << pid;
		prop["PID"] = stringConverter.str();
		prop["FILE_OUT"] = fileOut;
		prop["SO_NAME"] = so_name;
		prop["DFS_MASTER"] = dfsMaster;
		stringConverter.str("");
		stringConverter << dfsPort;
		prop["DFS_PORT"] = stringConverter.str();
		staleness = 0;
	};
};

#endif
