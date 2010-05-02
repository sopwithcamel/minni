#include "HDFS.h"

bool HDFS::connect()
{
	fs = hdfsConnect(host, port);
	if (fs == NULL) throw("Error connecting to HDFS."); 
}

bool HDFS::disconnect()
{
	if (hdfsDisconnect(fs)) throw("Error disconnecting to HDFS.");
}

bool HDFS::checkExistance(string path)
{
	if (hdfsExists(fs, path.c_str()))	return false;
	return true;
}

uint64_t HDFS::readChunkOffset(string path, uint64_t offset, string buf)
{
	uint64_t ret = 0;
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
	ret = (uint64_t) hdfsPread(fs, file, (tOffset) offset, buf.c_str(), (tSize) buf.length());
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

void HDFS::getChunkLocations(string path, ChunkID cid, vector<string*> & _return)
{
	/* dynamically size 2-d array, last pos is 0 */
	char*** 2dArray = char*** hdfsGetHosts(hdfsFS fs, const char* path, 
            tOffset start, tOffset length);
	
}

bool HDFS::createFile(string path)
{
	if(hdfsCreateDirectory(fs, path.c_str())) return false;
	return true;
}

uint64_t HDFS::appendToFile(string path, string buf)
{
	uint64_t ret = 0;
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_APPEND, 0, 0, 0);
	ret = (uint64_t)hdfsWrite(fs, file, buf.c_str(), buf.length());
	hdfsCloseFile(fs, file);
	return ret;
}

uint64_t HDFS::writeToFileOffset(string path, uint64_t offset, string buf)
{
	uint64_t ret = 0;
	hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
	ret = hdfsSeek(fs, file, (tOffset) offset); 
	if (ret) return ret;
	ret = (uint64_t) hdfsWrite(fs, file, buf.c_str(), buf.length());
	hdfsCloseFile(fs, file);
	return ret;
}
