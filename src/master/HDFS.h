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
		bool connect();
		bool disconnect();
		bool checkExistance(string path);							
		uint64_t readChunkOffset(string path, uint64_t offset, char* buf, uint64_t length);/* returns bytes read, -1 error */
		uint64_t getChunkSize(string path);
		uint64_t getNumChunks(string path);
		void getChunkLocations(string path, ChunkID cid, vector<string*> & _return);	/* returns server urls -- caller must clean up the vector, free memory  */
		bool createFile(string path);
		uint64_t appendToFile(string path, char* buf, uint64_t length);				/* returns bytes written, -1 error */
		uint64_t writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length);	/* returns bytes read, -1 error */
};

#endif
