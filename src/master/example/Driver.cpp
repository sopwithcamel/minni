#include "Master.h"
#include <unistd.h>
#include <string.h>

#define LAG 5
#define HDFS_TEST 1
#define PRODUCTION_TEST 0
#define HDFS_TEST_PATH "/test/foo.log"

int main(int argc, char* args[])
{
	if (PRODUCTION_TEST)
	{
		vector<string*> input;
		input.push_back(new string("/input/test.file"));
		string output = "/output/";
		string dfs_master = "localhost";
		string so_name = "wordcount.so";
		uint16_t dfs_port = 9000;
		JobID maxJobs = 10;
		JobID maxMaps = 5000;
		JobID maxReduces = 1000;
		MapReduceSpecification spec(input, output, dfs_master, so_name, dfs_port, maxJobs, maxMaps, maxReduces);
		HDFS hdfs("localhost", 9000);
		Master m(spec, hdfs, "example/nodes.conf");
		while (m.checkMapStatus()) /* assignment of all maps */
		{
			cout << "Assigning and running maps." << endl;
			m.assignMaps();
			m.checkStatus();
			m.assignReduces();
			sleep(LAG);
		}

		while (m.checkReducerStatus()) /* assignment of all reduces */
		{
			cout << "Assigning and running reduces." << endl;
			m.assignReduces();
			m.checkStatus();
			sleep(LAG);
		}

		while (!m.maps() || !m.reduces()) /* wait for running maps and reduces */
		{
			cout << "Waiting for all maps and reduces to finish." << endl;
			m.checkStatus();
			sleep(LAG);
		}
		cout << "Congratulations.  You finished an entire MapReduce Job." << endl;

		MapReduceResult result(m.getNumberOfMapsCompleted(), m.getNumberOfReducesCompleted(), m.getNumberOfNodes());

		/* return result; */

		return 0; /* return!!! */
	}

	if (HDFS_TEST)
	{
		char buf[256];
		char hello[12] = {'h','e','l','l','o',' ','w','o','r','l','d','\0'};
		char jello[12] = {'j','e','l','l','o',' ','w','a','l','l','a','\0'};
		memset(buf, 0, 256);
		HDFS hdfs("localhost", 9000);
		cout << "Connecting to DFS" << endl;
		hdfs.connect();
		cout << "Connected to DFS" << endl;
		cout << "Creating '" << HDFS_TEST_PATH << "': " << hdfs.createFile(HDFS_TEST_PATH) << endl;
		cout << "/test/foo.log exists: " << hdfs.checkExistance(HDFS_TEST_PATH) << endl;
		cout << "Writing \"jello walla\" to '" << HDFS_TEST_PATH "':"
			<< hdfs.writeToFile(HDFS_TEST_PATH, jello, strlen(jello)) << endl;
		cout << "Writing \"hello world\" to '" << HDFS_TEST_PATH << "': "
			<< hdfs.writeToFile(HDFS_TEST_PATH, hello, strlen(hello)) << endl;
		cout << "Closing file " << HDFS_TEST_PATH << ":" << hdfs.closeFile(HDFS_TEST_PATH) << endl;
		cout << "Reading from file '" << HDFS_TEST_PATH << "': " << hdfs.readChunkOffset(HDFS_TEST_PATH, (uint64_t)0, buf, (uint64_t)256)
			<< "\t" << buf << endl;
		cout << "Chunksize for '" << HDFS_TEST_PATH << "': " << hdfs.getChunkSize(HDFS_TEST_PATH) << endl;
		cout << "Chunks for '" << HDFS_TEST_PATH << "': " << hdfs.getNumChunks(HDFS_TEST_PATH) << endl;
		vector<string> _return;
		hdfs.getChunkLocations(HDFS_TEST_PATH, 0, _return);
		for(unsigned int i = 0; i < _return.size(); i++)
		{
			cout << "\tChunk at: " << _return[i] << endl;
		}
		hdfs.disconnect();
		return 0;
	}
}
