#ifndef MINNIE_MASTER_HDFS_H
#define MINNIE_MASTER_HDFS_H

#include "DFS.h"
#include "hdfs.h"
#include "daemon_types.h"
#include <math.h>
#include <string>
#include <vector>

using namespace std;
using namespace workdaemon;

/* NOT THREAD SAFE */
class HDFS : public DFS
{
	private:
		hdfsFS fs;
	public:
		HDFS(string host, uint16_t port = 9000) : DFS(host, port) {};
		bool connect();
		bool disconnect();
		bool checkExistance(string path);							
		int64_t readChunkOffset(string path, uint64_t offset, char* buf, uint64_t length);/* returns bytes read, -1 error */
		uint64_t getChunkSize(string path);
		uint64_t getNumChunks(string path);
		void getChunkLocations(string path, ChunkID cid, vector<string> & _return);	/* returns server urls -- caller must clean up the vector, free memory  */
		bool createFile(string path);
		int64_t appendToFile(string path, char* buf, uint64_t length);					/* DO NOT USE */
		int64_t writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length);	/* DO NOT USE */
		int64_t writeToFile(string path, char* buf, uint64_t length);					/* only approved writing method, -1 on error */
};

#endif
