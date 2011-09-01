#include "Reader.h"

int main(int argc, char* args[])
{
	if (argc < 5)
	{
		cout << USAGE << endl;
		return EXIT_FAILURE;
	}

	char* buf[BUFFER_SIZE];
	string localFile(args[1]);
	string dfsPath(args[2]);
	string metaServer(args[3]);
	istringstream in(args[4]);
	uint16_t port;
	int fd;
	uint64_t chunkSize, size, pos = 0;
	int64_t wrote;
	in >> port;
	KDFS dfs = KDFS(metaServer, port);
	fd = open(localFile.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 00644);
	
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

	while ((int64_t)size - (int64_t)BUFFER_SIZE >= 0)
	{
		wrote = dfs.readChunkOffset(dfsPath, pos, (char*) buf, BUFFER_SIZE);
		
		if (wrote < 0)
		{
			cout << "Error reading from DFS." << endl;
			return EXIT_FAILURE;
		}

		if (wrote == 0)
		{
			size = 0;
			break; /* EOF */
		}

		wrote = write(fd, buf, wrote);

		if (wrote < 0)
		{
			cout << "Error writing to local file." << endl;
			return EXIT_FAILURE;
		}

		size -= wrote; /* record written bytes */
		pos += wrote;
	}

	if (size > 0)
	{
		cout << "Writing " << size << " bytes" << endl;
		if ((wrote = dfs.readChunkOffset(dfsPath, pos, (char*) buf, size)) < 0)
		{
			cout << "Error on final read from DFS." << endl;
			return EXIT_FAILURE;
		}

		if ((wrote = write(fd, buf, size)) < 0)
		{
			cout << "Error on final write to file." << endl;
			return EXIT_FAILURE;
		}	
		pos += wrote;
	}

	dfs.closeFile(dfsPath);
	
	if (ftruncate(fd, pos) < 0)
	{
		cout << "Error truncating file to read byte size." << endl;
		return EXIT_FAILURE;
	}
	
	if (close(fd) < 0)
	{
		cout << "Error closing file descriptor for written out and truncated file." << endl;
		return EXIT_FAILURE;
	}

	cout << "File successfully read from DFS." << endl;

	return EXIT_SUCCESS;
}
