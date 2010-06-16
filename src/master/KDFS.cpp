#include "config.h"
#include "KDFS.h"

#include <iostream>
using namespace std;

KDFS::~KDFS()
{
	map<string, int>::iterator iter;
	for (iter = fileCache.begin(); iter != fileCache.end(); iter++)
	{
		fs->Close((*iter).second);
	}
}

bool KDFS::connect()
{
	fs = getKfsClientFactory()->GetClient(host, port);
	if (fs == NULL) throw("Error connecting to KDFS.");
	return true;
}

bool KDFS::disconnect()
{
	return true;
}

bool KDFS::checkExistence(string path)
{
	return fs->Exists(path.c_str());
}

int64_t KDFS::readChunkOffset(string path, uint64_t offset, char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd = fs->Open(path.c_str(), O_RDONLY);
	if (fd < 0) throw("Error opening file read only.");
	if (fs->Seek(fd, offset) < 0) throw("Error seeking to offset in file for read.");
	ret = (int64_t) fs->Read(fd, buf, (size_t) length);
	fs->Close(fd);
	return ret;
}

uint64_t KDFS::getChunkSize(string path)
{
	return fs->GetChunkSize(path.c_str());
}

uint64_t KDFS::getNumChunks(string path)
{
	return fs->GetNumChunks(path.c_str());
}

void KDFS::getChunkLocations(string path, ChunkID cid, vector<string> & _return)
{
	/*vector< vector<string> > locations;
	if (fs->GetDataLocation(path.c_str(), (off_t) (cid*getChunkSize(path)), (off_t) getChunkSize(path), &locations) > 0)
	{
		for (int i = 0; i < locations.size(); i++)
		{
			for (int j = 0; j < locations[i].size(); j++)
			{
				cout << "DEBUG: Found chunk[" << cid << "] location server " << locations[i][j] << endl;
				_return.push_back(locations[i][j]);
			}
		}		
	}
	else
	{
		throw("Error looking up locations for chunk.");
	}*/
}

bool KDFS::createFile(string path)
{
	int fd;
	if (fd = fs->Create(path.c_str(), 3, true) < 0)
	{
		return false;
	}
	else
	{
		fs->Close(fd);
		return true;
	}
}

/* dangerous and unsafe */
int64_t KDFS::appendToFile(string path, char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd = fs->Open(path.c_str(), O_WRONLY);
	if (fd < 0) return -1;
	ret = fs->AtomicRecordAppend(fd, buf, (int)length);
	if (fs->Close(fd) < 0) return -1;
	return ret;
}

int64_t KDFS::writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd = fs->Open(path.c_str(), O_WRONLY);
	if (fd < 0) throw("Error opening file write only.");
	if (fs->Seek(fd, offset) < 0) throw("Error seeking to offset in file for writing to offset.");
	ret = (int64_t) fs->Write(fd, buf, (size_t) length);
	if (fs->Close(fd) < 0) throw("Error closing file when writing to offest.");
	return ret;
}

int64_t KDFS::writeToFile(string path, const char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd;
	if (fileCache.find(path) != fileCache.end())
	{
		cout << "Pulling open file from fileCache" << endl;
		fd = fileCache[path];
	}
	else
	{
		cout << "Opening file for writing." << endl;
		if (fd = fs->Open(path.c_str(), O_WRONLY) > 0)
		{
			fileCache[path] = fd;
		}
		else
		{
			throw("Error opening file for writing.");
		}
	}
	ret = fs->Write(fd, buf, (size_t) length);
	return ret;
}

int64_t KDFS::closeFile(string path)
{
	int64_t ret;
	if (fileCache.find(path) != fileCache.end())
	{
		ret = fs->Close(fileCache[path]);
		fileCache.erase(path);
		return ret;
	}
	else
	{
		return -1;
	}
}
