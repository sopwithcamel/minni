#include "common.h"
#include "Master.h"
#include <unistd.h>
#include <string.h>
#include "util.h"
#include "Defs.h"

#define LAG 3
#define KDFS_TEST 0
#define PRODUCTION_TEST 1
#define KDFS_TEST_PATH "/test/foo.log"

int main(int argc, char* args[])
{
	if (PRODUCTION_TEST)
	{
		Config cfg;
		assert(openConfigFile(cfg));

		Setting& c_input = readConfigFile(cfg, "minni.input_files");
		string input = (const char*)c_input;

		Setting& c_output = readConfigFile(cfg, "minni.output_files");
		string output = (const char*)c_output;

		Setting& c_dfsmaster = readConfigFile(cfg, "minni.dfs.master");
		string dfs_master = (const char*)c_dfsmaster;

		Setting& c_dfsport = readConfigFile(cfg, "minni.dfs.port");
		uint16_t dfs_port = (int)c_dfsport;

		Setting& c_soname = readConfigFile(cfg, "minni.so_name");
		string so_name = (const char*)c_soname;

		Setting& c_nodesfile = readConfigFile(cfg, "minni.common.nodes_file");
		string nodes_file = (const char*)c_nodesfile;

		Setting& c_maxmaps = readConfigFile(cfg, "minni.max_maps");
		JobID maxMaps = (int)c_maxmaps;

		Setting& c_maxreduces = readConfigFile(cfg, "minni.max_reduces");
		JobID maxReduces = (int)c_maxreduces;
		JobID maxJobs = maxMaps + maxReduces;

		MapReduceSpecification spec = MapReduceSpecification();
		spec.addInput(input);
		spec.setOutputPath(output);
		spec.setDfsMaster(dfs_master);
		spec.setSoName(so_name);
		spec.setDfsPort(dfs_port);
		spec.setMaxJobsPerNode(maxJobs);
		spec.setMaxMaps(maxMaps);
		spec.setMaxReduces(maxReduces);
		KDFS hdfs(dfs_master.c_str(), dfs_port);
		Master m(&spec, hdfs, nodes_file.c_str());
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
