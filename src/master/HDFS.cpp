#include "HDFS.h"

#include <iostream>
using namespace std;

HDFS::~HDFS()
{
	map<string, hdfsFile>::iterator iter;
	for (iter = fileCache.begin(); iter != fileCache.end(); iter++)
	{
		hdfsCloseFile(fs, (*iter).second);
	}
	if (fs) disconnect();
}

bool HDFS::connect()
{
	fs = hdfsConnect(host.c_str(), port);
	if (fs == NULL) throw("Error connecting to HDFS.");
	return true;
}

bool HDFS::disconnect()
{
	if (hdfsDisconnect(fs)) throw("Error disconnecting from HDFS.");
	fs = NULL;
	return true;
}

bool HDFS::checkExistance(string path)
{
	if (hdfsExists(fs, path.c_str()))	return false;
	return true;
}

int64_t HDFS::readChunkOffset(string path, uint64_t offset, char* buf, uint64_t length)
{
	uint64_t ret = 0;
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
	ret = (uint64_t) hdfsPread(fs, file, (tOffset) offset, buf, (tSize) length);
	hdfsCloseFile(fs, file);
	return ret;
}

uint64_t HDFS::getChunkSize(string path)
{
	hdfsFileInfo * fileInfoPtr = hdfsGetPathInfo(fs, path.c_str());
	if (fileInfoPtr == NULL) throw("Error getting chunk size on file.");
	uint64_t ret = (uint64_t) fileInfoPtr->mBlockSize;
	hdfsFreeFileInfo(fileInfoPtr, 1);
	return ret;
}

uint64_t HDFS::getNumChunks(string path)
{
	hdfsFileInfo * fileInfoPtr = hdfsGetPathInfo(fs, path.c_str());
	if (fileInfoPtr == NULL) throw("Error getting number of chunks in file.");
	uint64_t ret = (uint64_t) ceil((double)fileInfoPtr->mSize / (double)fileInfoPtr->mBlockSize);
	hdfsFreeFileInfo(fileInfoPtr, 1);
	return ret;
}

/* Reverse Engineered char*** format:
	Record layout:
		[char* node][int block][guard]
		[char* node][int block][guard]
		...
*/
void HDFS::getChunkLocations(string path, ChunkID cid, vector<string> & _return)
{
	/* dynamically size 2-d array, last pos is NULL */
	char*** twoDArray = hdfsGetHosts(fs, path.c_str(), cid*getChunkSize(path), getChunkSize(path));

	while (twoDArray != NULL)
	{
		cout << "DEBUG: " << twoDArray[0][0] << "|\t|" << (uint64_t)twoDArray[0][1] << endl;
		if ((int64_t)twoDArray[0][1] == cid)
		{
			_return.push_back(string(twoDArray[0][0]));
		}
		if (twoDArray[0][2] == NULL) break;
		twoDArray++;
	}
	hdfsFreeHosts(twoDArray);
}

bool HDFS::createFile(string path)
{
	//if(hdfsCreateDirectory(fs, path.c_str())) return false;
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
	if (file == NULL) return false;
	if (hdfsCloseFile(fs, file)) return false;
	return true;
}

/* dangerous and unsafe */
int64_t HDFS::appendToFile(string path, char* buf, uint64_t length)
{
	uint64_t ret = 0;
	hdfsFileInfo * fileInfo = hdfsGetPathInfo(fs, path.c_str());
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDWR || (O_EXCL & O_CREAT), 0, 0, 0);
	if (fileInfo->mSize > 0) ret = hdfsSeek(fs, file, fileInfo->mSize);
	if (ret) return ret; /* error seeking */
	ret = (uint64_t)hdfsWrite(fs, file, buf, (tSize) length);
	hdfsFreeFileInfo(fileInfo, 1);
	if (hdfsCloseFile(fs, file)) return -1;
	return ret;
}

int64_t HDFS::writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length)
{
	uint64_t ret = 0;
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDWR || (O_EXCL & O_CREAT), 0, 0, 0);
	if (offset > 0) ret = hdfsSeek(fs, file, (tOffset) offset);
	if (ret) return ret;
	ret = (uint64_t) hdfsWrite(fs, file, buf, (tSize) length);
	if (hdfsCloseFile(fs, file)) return -1;
	return ret;
}

int64_t HDFS::writeToFile(string path, char* buf, uint64_t length)
{
	uint64_t ret = 0;
	hdfsFile file;
	if (fileCache.find(path) != fileCache.end())
	{
		cout << "Pulling open file from fileCache" << endl;
		file = fileCache[path];
	}
	else
	{
		cout << "Opening file for writing." << endl;
		file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
		fileCache[path] = file;
	}
	ret = hdfsWrite(fs, file, buf, (tSize) length);
	return ret;
}

int64_t HDFS::closeFile(string path)
{
	int64_t ret;
	if (fileCache.find(path) != fileCache.end())
	{
		ret = hdfsCloseFile(fs, fileCache[path]);
		fileCache.erase(path);
		return ret;
	}
	else
	{
		return -1;
	}
}
