#include "Reader.h"

int main(int argc, char* args[])
{
	if (argc < 5)
	{
		cout << USAGE << endl;
		return EXIT_FAILURE;
	}

	void* mmapFile;
	string localFile(args[1]);
	string dfsPath(args[2]);
	string metaServer(args[3]);
	istringstream in(args[4]);
	uint16_t port;
	int fd;
	off_t size;
	uint64_t chunkSize, pos = 0;
	int64_t wrote;
	in >> port;
	KDFS dfs = KDFS(metaServer, port);
	fd = open(localFile.c_str(), O_CREAT | O_RDWR, 00644);
	
	if (fd < 0)
	{
		cout << "Error opening file." << endl;
	}

	if (!dfs.connect())
	{
		cout << "Error connecting to KDFS." << endl;
	}

	cout << "Copying " << dfsPath << " to " << localFile << " from " << metaServer << ":" << port << endl;

	chunkSize = dfs.getChunkSize(dfsPath);
	
	cout << "Chunk Size [bytes]: " << chunkSize << endl;
	
	size = chunkSize * dfs.getNumChunks(dfsPath);

	cout << "File Size [bytes]: " << size << endl;

	lseek(fd, size-1, SEEK_SET);
	if (write(fd, (char *)&pos, 1) != 1)
	{
		cout << "Error preallocating file space to write into." << endl;
		return EXIT_FAILURE;
	}
	lseek(fd, 0, SEEK_SET);
	mmapFile = mmap(NULL, size, PROT_WRITE, MAP_SHARED, fd, 0);

	if (mmapFile == MAP_FAILED)
	{
		cout << "Failed opening " << localFile << " for memory mapped IO" << endl;
		return EXIT_FAILURE;
	}

	while ((int64_t)size - (int64_t)chunkSize >= 0)
	{
		cout << "Writing a chunk..." << endl;
		wrote = dfs.readChunkOffset(dfsPath, pos, &(((char*)mmapFile)[pos]), chunkSize);
		if (wrote < 0)
		{
			cout << "Error writing chunk." << endl;
			return EXIT_FAILURE;
		}
		size -= wrote; /* record written bytes */
		pos += wrote; /* move pointer up */
	}

	if (size > 0)
	{
		cout << "Writing " << size << " bytes" << endl;
		if ((wrote = dfs.readChunkOffset(dfsPath, pos, &(((char*)mmapFile)[pos]), size)) < 0)
		{
			cout << "Error on final read/write." << endl;
			return EXIT_FAILURE;
		}
		pos += wrote;
	}

	dfs.closeFile(dfsPath);

	munmap(mmapFile, pos);
	return EXIT_SUCCESS;
}
