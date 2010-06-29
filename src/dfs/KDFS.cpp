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

/* KFS doesn't appear to have an explicit shutdown or disconnect */
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
	int fd = openFileCacheLookup(path, O_RDONLY);
	if (fs->Seek(fd, offset) < 0) throw("Error seeking to offset in file for read.");
	ret = (int64_t) fs->Read(fd, buf, (size_t) length);
	if (ret < 0) cout << "ERROR: " << ErrorCodeToStr(ret) << endl;
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
	vector< vector<string> > locations;
	if (fs->GetDataLocation(path.c_str(), (off_t) (cid*(this->getChunkSize(path))), (off_t) (this->getChunkSize(path)), locations) > 0)
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
	}
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
		/* unknown options on this fd (under-specified), just close */
		fs->Close(fd);
		return true;
	}
}

int64_t KDFS::appendToFile(string path, char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd = openFileCacheLookup(path, O_RDWR);
	ret = fs->AtomicRecordAppend(fd, buf, (int)length);
	return ret;
}

int64_t KDFS::writeToFileOffset(string path, uint64_t offset, char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd = openFileCacheLookup(path, O_RDWR);
	if (fs->Seek(fd, offset) < 0) throw("Error seeking to offset in file for writing to offset.");
	ret = (int64_t) fs->Write(fd, buf, (size_t) length);
	return ret;
}

/* TODO: should this just be append? */
int64_t KDFS::writeToFile(string path, const char* buf, uint64_t length)
{
	int64_t ret = 0;
	int fd = openFileCacheLookup(path, O_RDWR);
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

int KDFS::openFileCacheLookup(string &path, int options)
{
		int fd;
		if (fileCache.find(path) != fileCache.end())
		{
			fd = fileCache[path];
		}
		else
		{
			cout << "Opening file." << endl;
			if ((fd = fs->Open(path.c_str(), options)) >= 0)
			{
				fileCache[path] = fd;
			}
			else
			{
				cout << "Failed opening " << path << " with options " << options << " fd is " << fd << endl;
				throw("Error opening file, missed cache.");
			}
		}
		return fd;
}
