#ifndef MINNIE_MASTER_DFS_H
#define MINNIE_MASTER_DFS_H

#include <string>

#include "daemon_types.h"

using namespace workdaemon;
using namespace std;

/* NOT assumed to be thread safe... */
class DFS
{
	public:
		DFS(string host, uint16_t port) : host(host), port(port) {};
		~DFS();
		virtual bool connect() = 0;
		virtual bool disconnect() = 0;
		virtual bool checkExistance(string path) = 0;
		virtual uint64_t readChunkOffset(string path, uint64_t offset, string buf) = 0;
		virtual uint64_t getChunkSize(string path) = 0;
		virtual uint64_t getNumChunks(string path) = 0;
		virtual vector<string> getChunkLocations(string path, ChunkID cid) = 0;
		virtual bool createFile(string path) = 0;
		virtual uint64_t appendToFile(string path, string buf) = 0;
		virtual uint64_t writeToFileOffset(string path, uint64_t offset, string buf) = 0;
	protected:
		string host;
		uint16_t port;
};

#endif
