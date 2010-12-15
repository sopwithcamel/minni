#include "common.h"
#include "Master.h"
#include <unistd.h>
#include <string.h>

#define LAG 3
#define KDFS_TEST 0
#define PRODUCTION_TEST 1
#define KDFS_TEST_PATH "/test/foo.log"

int main(int argc, char* args[])
{
	if (PRODUCTION_TEST)
	{
//		string input = "/medgutfile.txt";
		string input = "/500x26e4x12.txt";
//		string input = "/shake.txt";
		//string input = "/input/10GB_random.dat"; 
		string output = "/";
		string dfs_master = "127.0.0.1";
		string so_name = "wordcount.so";
		uint16_t dfs_port = 20000;
		JobID maxJobs = 2;
		JobID maxMaps = 1;
		JobID maxReduces = 1;
		MapReduceSpecification spec = MapReduceSpecification();
		spec.addInput(input);
		spec.setOutputPath(output);
		spec.setDfsMaster(dfs_master);
		spec.setSoName(so_name);
		spec.setDfsPort(dfs_port);
		spec.setMaxJobsPerNode(maxJobs);
		spec.setMaxMaps(maxMaps);
		spec.setMaxReduces(maxReduces);
		KDFS hdfs("127.0.0.1", 20000);
		Master m(&spec, hdfs, "example/nodes.conf");
		while (m.checkMapStatus()) /* assignment of all maps */
		{
			cout << "Assigning and running maps." << endl;
			m.assignMaps();
			m.checkStatus();
			m.assignReduces();
			m.checkState();
			sleep(LAG);
		}

		while (m.checkReducerStatus()) /* assignment of all reduces */
		{
			cout << "Assigning and running reduces." << endl;
			m.assignReduces();
			m.checkStatus();
			m.checkState();
			sleep(LAG);
		}

		while (m.maps() || m.reduces()) /* wait for running maps and reduces */
		{
			cout << "Waiting for all maps and reduces to finish." << endl;
			m.checkStatus();
			m.assignMaps();
			m.assignReduces();
			m.checkState();
			sleep(LAG);
		}
		cout << "Congratulations.  You finished an entire MapReduce Job. [" << m.getNumberOfMapsCompleted() << " maps, " << m.getNumberOfReducesCompleted() << " reduces, " << m.getNumberOfNodes() << " nodes]" << endl;

		MapReduceResult result(m.getNumberOfMapsCompleted(), m.getNumberOfReducesCompleted(), m.getNumberOfNodes());
		/* return result; */
		return 0; /* return!!! */
	}

	if (KDFS_TEST)
	{
		char buf[256];
		char hello[12] = {'h','e','l','l','o',' ','w','o','r','l','d','\0'};
		char jello[12] = {'j','e','l','l','o',' ','w','a','l','l','a','\0'};
		memset(buf, 0, 256);
		KDFS hdfs("localhost", 40000);
		cout << "Connecting to DFS" << endl;
		hdfs.connect();
		cout << "Connected to DFS" << endl;
		cout << "Creating '" << KDFS_TEST_PATH << "': " << hdfs.createFile(KDFS_TEST_PATH) << endl;
		cout << "/test/foo.log exists: " << hdfs.checkExistence(KDFS_TEST_PATH) << endl;
		cout << "Writing \"jello walla\" to '" << KDFS_TEST_PATH "':"
			<< hdfs.writeToFile(KDFS_TEST_PATH, jello, strlen(jello)) << endl;
		cout << "Writing \"hello world\" to '" << KDFS_TEST_PATH << "': "
			<< hdfs.writeToFile(KDFS_TEST_PATH, hello, strlen(hello)) << endl;
		cout << "Closing file " << KDFS_TEST_PATH << ":" << hdfs.closeFile(KDFS_TEST_PATH) << endl;
		cout << "Reading from file '" << KDFS_TEST_PATH << "': " << hdfs.readChunkOffset(KDFS_TEST_PATH, (uint64_t)0, buf, (uint64_t)256)
			<< "\t" << buf << endl;
		cout << "Chunksize for '" << KDFS_TEST_PATH << "': " << hdfs.getChunkSize(KDFS_TEST_PATH) << endl;
		cout << "Chunks for '" << KDFS_TEST_PATH << "': " << hdfs.getNumChunks(KDFS_TEST_PATH) << endl;
		vector<string> _return;
		hdfs.getChunkLocations(KDFS_TEST_PATH, 0, _return);
		for(unsigned int i = 0; i < _return.size(); i++)
		{
			cout << "\tChunk at: " << _return[i] << endl;
		}
		hdfs.disconnect();
		return 0;
	}
}
