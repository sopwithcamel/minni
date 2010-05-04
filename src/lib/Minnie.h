#ifndef Minnie_H
#define Minnie_H
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include "daemon_types.h"

using namespace workdaemon;
using namespace std;

class MapReduceSpecification {
  public:
        MapReduceSpecification() {};
	~MapReduceSpecification() {};
	vector<string> getInputFiles() { return input; };
	string getOutputPath() { return output; };
	string getDfsMaster() {return dfs_master; };
	string getSoName() { return so_name; };
	uint16_t getDfsPort() { return dfs_port; };
	JobID getMaxJobsPerNode() { return maxJobs; };
	JobID getMaxMaps() { return maxMaps; };
	JobID getMaxReduces() { return maxReduces; }

	void setOutputPath(string o) { output = o;}
	void setDfsMaster(string master) {dfs_master = master; };
	void setSoName(string sname) { so_name = sname; };
	void setDfsPort(uint16_t port) { dfs_port = port;};
	void setMaxJobsPerNode(JobID maxj) { maxJobs = maxj;};
	void setMaxMaps(JobID maxm) { maxMaps = maxm;};
	void setMaxReduces(JobID maxr) {maxReduces = maxr;};
	void addInput(string in) {input.push_back(in);};

  private:
	vector<string> input;
	string output;
        string dfs_master;
	string so_name;
	uint16_t dfs_port;
	JobID maxJobs;
	JobID maxMaps;
	JobID maxReduces;
};

class MapReduceResult
{
        public:
                MapReduceResult(JobID completedMaps, JobID completedReduces, JobID numberNodes) : completedMaps(completedMaps), completedReduces(completedReduces), numberNodes(numberNodes) {};
                JobID getCompletedMaps() { return completedMaps; };
                JobID getCompletedReduces() { return completedReduces; };
                JobID getNumberNodes() { return numberNodes; };
        private:
                JobID completedMaps;
                JobID completedReduces;
                JobID numberNodes;
};

#endif //Minnie_H_
