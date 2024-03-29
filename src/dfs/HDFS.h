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
/* NOTE: DO not try and read/write from the same file.  The internal fileCache
prevents this */
/* NOTE: ALWAYS close a file after writing and before reading (if you're reading
in the same thread). */
class HDFS : public DFS
{
	private:
		hdfsFS fs;
		map<string, hdfsFile> fileCache;
	public:
		HDFS(string host, uint16_t port = 9000) : DFS(host, port) {};					/* constructor */
		~HDFS();															/* destructor */
		bool connect();													/* connect to the DFS */
		bool disconnect();													/* shutdown the DFS connection */
		bool checkExistence(string path);										/* check if a file exists in the DFS */
		int64_t readChunkOffset(string path, uint64_t offset, char* buf, uint64_t length);	/* returns bytes read, -1 error */
		uint64_t getChunkSize(string path);									/* get the size of a chunk of a given file */
		uint64_t getNumChunks(string path);									/* get the number of chunks in a file */
		void getChunkLocations(string path, ChunkID cid, vector<string> & _return);	/* 	returns server urls -- caller must clean up the
																			vector, free memory  */
		bool createFile(string path);											/* creates a file, false on failure */
		//@BEGIN UNSUPPORTED
		int64_t appendToFile(string path, char* buf, uint64_t length);					/* DO NOT USE */
		int64_t writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length);	/* DO NOT USE */
		//@END UNSUPPORTED
		int64_t writeToFile(string path, const char* buf, uint64_t length);					/* only approved writing method, -1 on error */
		int64_t closeFile(string path);											/* closes a file after writing */
		bool isDirectory(string path);
		void getDirSummary(string path, uint64_t& num_fil, uint64_t& num_byt);
};

#endif
