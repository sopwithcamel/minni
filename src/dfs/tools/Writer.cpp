#include "config.h"
#include "Writer.h"

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
	struct stat64 st;
	uint64_t size;
	int64_t wrote;
	in >> port;
	KDFS dfs(metaServer, port);
	fd = open64(localFile.c_str(), O_RDONLY);
	
	if (!dfs.connect())
	{
		cout << "Error connecting to KDFS." << endl;
	}

	cout << "Copying " << localFile << " to " << dfsPath << " on " << metaServer << ":" << port << endl;

	stat64(localFile.c_str(), &st);
	size = st.st_size;
	cout << "File Size [bytes]: " << size << endl;

	if (!dfs.createFile(dfsPath))
	{
		cout << "Error creating file " << dfsPath << endl;
		return EXIT_FAILURE;
	}

	while ((int64_t)size - (int64_t)BUFFER_SIZE >= 0)
	{
		if (read(fd, buf, BUFFER_SIZE) != BUFFER_SIZE)
		{
			cout << "Error reading from file to buffer." << endl;
			return EXIT_FAILURE;
		}

		if (dfs.writeToFile(dfsPath, (const char*)buf, BUFFER_SIZE) != BUFFER_SIZE)
		{
			cout << "Error writing buffer to DFS." << endl;
			return EXIT_FAILURE;
		}

		size -= BUFFER_SIZE; /* record written bytes */
	}

	if (size > 0)
	{
		cout << "Writing " << size << " bytes" << endl;
		if (read(fd, buf, size) != size)
		{
			cout << "Error reading from file to buffer." << endl;
			return EXIT_FAILURE;
		}

		if (dfs.writeToFile(dfsPath, (const char*)buf, size) != size)
		{
			cout << "Error writing buffer to DFS." << endl;
			return EXIT_FAILURE;
		}

	}

	dfs.closeFile(dfsPath);

	if (close(fd) < 0)
	{
		cout << "Error closing file descriptor from read." << endl;
		return EXIT_FAILURE;
	}

	cout << "File successfully written to DFS." << endl;
	
	return EXIT_SUCCESS;
}
