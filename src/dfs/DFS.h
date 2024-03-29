#ifndef MINNIE_MASTER_DFS_H
#define MINNIE_MASTER_DFS_H

#include <string>

#include "daemon_types.h"

using namespace workdaemon;
using namespace std;

/* NOT assumed to be thread safe... */
class DFS
{
	private:
		virtual int openFileCacheLookup(string &path, int options) = 0;
	public:
		DFS(string host, uint16_t port) : host(host), port(port) {};
		~DFS() {};
		virtual bool connect() = 0;
		virtual bool disconnect() = 0;
		virtual bool checkExistence(string path) = 0;
		virtual int64_t readChunkOffset(string path, uint64_t offset, char* buf, uint64_t length) = 0;
		virtual uint64_t getChunkSize(string path) = 0;
		virtual uint64_t getNumChunks(string path) = 0;
		virtual void getChunkLocations(string path, ChunkID cid, vector<string> & _return) = 0;
		virtual bool createFile(string path) = 0;
		virtual int64_t appendToFile(string path, char* buf, uint64_t length) = 0;
		virtual int64_t writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length) = 0;
		virtual int64_t writeToFile(string path, const char* buf, uint64_t length) = 0;
		virtual int64_t closeFile(string path) = 0;
		virtual bool isDirectory(string path) = 0;
		virtual void getDirSummary(string path, uint64_t& num_fil, uint64_t& num_byt) = 0;
		virtual int64_t readDir(string path, vector<string>& conts) = 0;
		virtual int64_t readFile(string path, char*& buf, size_t& fil_size) = 0;
	protected:
		string host;
		uint16_t port;
};

#endif
