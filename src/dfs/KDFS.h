#ifndef MINNIE_MASTER_KDFS_H
#define MINNIE_MASTER_KDFS_H

#include "DFS.h"
#include "kfs/KfsClient.h"
#include "daemon_types.h"
#include <fcntl.h>
#include <math.h>
#include <string>
#include <vector>

using namespace std;
using namespace workdaemon;
using namespace KFS;

/* NOT THREAD SAFE */
/* NOTE: DO not try and read/write from the same file.  The internal fileCache
prevents this */
/* NOTE: ALWAYS close a file after writing and before reading (if you're reading
in the same thread). */
class KDFS : public DFS
{
	private:
		KfsClientPtr fs;
		map<string, int> fileCache;
		int openFileCacheLookup(string &path, int options);
	public:
		/* constructor */
		KDFS(string host, uint16_t port = 9000) : DFS(host, port) {};
		/* destructor */
		~KDFS();
		/* connect to the DFS */
		bool connect();
		/* shutdown the DFS connection */
		bool disconnect();									
		/* check if a file exists in the DFS */
		bool checkExistence(string path);
		/* returns bytes read, -1 error */
		int64_t readChunkOffset(string path, uint64_t offset,
				char* buf, uint64_t length);
		/* get the size of a chunk of a given file */
		uint64_t getChunkSize(string path);
		/* get the number of chunks in a file */
		uint64_t getNumChunks(string path);
		/* get server urls -- caller must clean vector, free memory */
		void getChunkLocations(string path, ChunkID cid, vector<string> & _return);
		/* creates a file, false on failure */
		bool createFile(string path);
		/* untested with KFS now */
		int64_t appendToFile(string path, char* buf, uint64_t length);
		/* untested with KFS now */
		int64_t writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length);
		/* only approved writing method, -1 on error */
		int64_t writeToFile(string path, const char* buf, uint64_t length);				
		/* closes a file after writing */
		int64_t closeFile(string path);
		bool isDirectory(string path);
		void getDirSummary(string path, uint64_t& num_fil, uint64_t& num_byt);
		int64_t readDir(string path, vector<string>& conts);
		/* allocates a buffer and reads a file. Caller must free */
		int64_t readFile(string path, char** buf);
};

#endif
