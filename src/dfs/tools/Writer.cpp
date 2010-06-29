#include "Writer.h"

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
	struct stat st;
	off_t size;
	uint64_t chunkSize, pos = 0;
	int64_t wrote;
	in >> port;
	KDFS dfs(metaServer, port);
	fd = open(localFile.c_str(), O_RDONLY);
	
	if (!dfs.connect())
	{
		cout << "Error connecting to KDFS." << endl;
	}

	cout << "Copying " << localFile << " to " << dfsPath << " on " << metaServer << ":" << port << endl;

	stat(localFile.c_str(), &st);
	size = st.st_size;
	cout << "File Size [bytes]: " << size << endl;
	mmapFile = mmap(NULL, size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);

	if (mmapFile == MAP_FAILED)
	{
		cout << "Failed opening " << localFile << " for memory mapped IO" << endl;
	}

	if (!dfs.createFile(dfsPath))
	{
		cout << "Error creating file " << dfsPath << endl;
		return EXIT_FAILURE;
	}

	chunkSize = dfs.getChunkSize(dfsPath);

	cout << "Chunk Size [bytes]: " << chunkSize << endl;

	while ((int64_t)size - (int64_t)chunkSize >= 0)
	{
		cout << "Writing a chunk..." << endl;
		wrote = dfs.writeToFile(dfsPath, &(((char*)mmapFile)[pos]), chunkSize);
		if (wrote != chunkSize)
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
		dfs.writeToFile(dfsPath, &(((char*)mmapFile)[pos]), size);
	}

	dfs.closeFile(dfsPath);

	munmap(mmapFile, st.st_size);
	return EXIT_SUCCESS;
}
