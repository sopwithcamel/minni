#include "Master.h"
#include <unistd.h>
#include <string.h>

#define LAG 5
#define MASTER_TEST 0
#define HDFS_TEST 0
#define PRODUCTION_TEST 1
#define HDFS_TEST_PATH "/test/foo.log"

int main(int argc, char* args[])
{
	if (MASTER_TEST)
	{
		Master m;
		m.loadNodesFile("example/nodes.conf");

		m.sendMapCommand();
		sleep(LAG);
		m.checkStatus();
		sleep(LAG);
		m.sendFinishedNodes();
		sleep(LAG);
		m.sendReduceCommand();
		sleep(LAG);
		m.checkStatus();
		sleep(LAG);
		return 0;
	}

	if (PRODUCTION_TEST)
	{
		MapReduceSpecification spec;
		Master m(spec);
		m.loadNodesFile("example/nodes.conf");
		while (m.checkMapStatus()) /* assignment of all maps */
		{
			cout << "Assigning and running maps." << endl;
			m.assignMaps();
			n.checkStatus();
			m.assignReduces();
			sleep(LAG);
		}

		while (m.checkReduceStatus()) /* assignment of all reduces */
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

		MapReduceResult result(m.getNumberOfCompletedMaps(), m.getNumberOfReducesCompleted(), m.getNumberOfNodes());

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
		hdfs.connect();
		cout << "Creating '" << HDFS_TEST_PATH << "': " << hdfs.createFile(HDFS_TEST_PATH) << endl;
		cout << "/test/foo.log exists: " << hdfs.checkExistance(HDFS_TEST_PATH) << endl;
		cout << "Writing \"jello walla\" to '" << HDFS_TEST_PATH "':"
			<< hdfs.writeToFile(HDFS_TEST_PATH, jello, strlen(jello)) << endl;
		cout << "Reading from file '" << HDFS_TEST_PATH << "': " << hdfs.readChunkOffset(string(HDFS_TEST_PATH), (uint64_t)0, buf, (uint64_t)256)
			<< "\t" << buf << endl;
		memset(buf, 0, 256);
		cout << "Writing \"hello world\" to '" << HDFS_TEST_PATH << "': "
			<< hdfs.writeToFile(HDFS_TEST_PATH, hello, strlen(hello)) << endl;
		cout << "Reading from file '" << HDFS_TEST_PATH << "': " << hdfs.readChunkOffset(string(HDFS_TEST_PATH), (uint64_t)0, buf, (uint64_t)256)
			<< "\t" << buf << endl;
		cout << "Chunksize for '" << HDFS_TEST_PATH << "': " << hdfs.getChunkSize(HDFS_TEST_PATH) << endl;
		cout << "Chunks for '" << HDFS_TEST_PATH << "': " << hdfs.getNumChunks(HDFS_TEST_PATH) << endl;
		vector<string*> _return;
		hdfs.getChunkLocations(HDFS_TEST_PATH, 0, _return);
		for(unsigned int i = 0; i < _return.size(); i++)
		{
			cout << "\tChunk at: " << *_return[i] << endl;
		}
		for(unsigned int i = 0; i < _return.size(); i++)
		{
			delete _return[i];
		}
		hdfs.disconnect();
		return 0;
	}
}
