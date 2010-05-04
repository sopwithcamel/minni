#ifndef MINNIE_MASTER_H
#define MINNIE_MASTER_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>

using namespace std;

#include "Communicator.h"
#include "Node.h"
#include "HDFS.h"
#include "Minnie.h"

class MapReduceSpecification
{
	public:
		MapReduceSpecification(vector<string*> input, string output, 
						string dfs_master, string so_name, uint16_t dfs_port,
						JobID maxJobs, JobID maxMaps, 
						JobID maxReduces): inputPath(input),
						outputPath(output), dfs_master(dfs_master),
						so_name(so_name), dfs_port(dfs_port), maxJobs(maxJobs),
						maxMaps(maxMaps), maxReduces(maxReduces) {};
		~MapReduceSpecification();
		vector<string*> getInputPath() { return inputPath; };
		string getOutputPath() { return outputPath; };
		string getDfsMaster() {return dfs_master; };
		string getSoName() { return so_name; };
		uint16_t getDfsPort() { return dfs_port; };
		JobID getMaxJobsPerNode() { return max_jobs; };
		JobID getMaxMaps() { return max_maps; };
		JobID getMaxReduces() { return max_reduces; };

	private:
		vector<string*> inputPath;
		string outputPath;
		string dfs_master;
		string so_name;
		uint16_t dfs_port;
		
		JobID max_jobs;
		JobID max_maps;
		JobID max_reduces;
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

class Master {
	public:
		Master(MapReduceSpecification spec, DFS dfs, string nodesFile); 	/* constructor */
		~Master();											/* destructor */
		void assignMaps();										/* assign and run as many maps as possible */
		void assignReduces();									/* assign and run as many reduces as possible */
		bool checkStatus();										/* poll all nodes for status */
		bool checkMapStatus();									/* check if all maps have completed */
		bool checkReducerStatus();								/* check if all reduces have completed */
		void sendFinishedNodes();								/* send list of finished URL's */
		bool maps();											/* check if map jobs still remain to be assigned */
		bool reduces();										/* check if reduce jobs still remain to be assigned */
		JobID getNumberOfNodes();								/* return number of nodes used */
		JobID getNumberOfMapsCompleted();						/* return number of maps finished */
		JobID getNumberOfReducesCompleted();					/* return number of reduces finished */
	private:
		void assignMapJob(Node* node, ChunkID cid, string fileIn);		/* assign a map job to a node */
		void assignReduceJob(Node* node, PartitionID pid, string fileOut);	/* assign a reduce job to a node */
		void updateMaximumNode();								/* loops through all nodes and updates the maximum */
		void loadNodesFile(string fileName);						/* load node names from file */
		void sendAllMappersFinished();							/* send all maps finished to all nodes */
		map<string, Node*> nodes;								/* maintain string list of all node names */
		Node* nodeWithMaxMapJobs;								/* node with maximum remaining map jobs */
		uint64_t maximumMapJobsCount;							/* the number of jobs assigned to highest map loaded node */
		Node* nodeWithMaxReduceJobs;							/* node with maximum remaining reduce jobs */
		uint64_t maximumReduceJobsCount;						/* the number of jobs assigned to highest reduce loaded node */
		vector<string> finishedNodes;							/* string list of finished node URL's */
		set<string> alreadySentFinishedNodes;						/* set of finished nodes we've sent out */
		JobID jidCounter;										/* monotonically increasing universal job counter */
		JobID activeMappers;									/* count of active mappers */
		JobID activeReducers;									/* count of active reducers */
		JobID remainingMappers;								/* remaining map jobs to run */
		JobID remainingReducers;								/* remaining reduce jobs to run */
		JobID completedMaps;									/* successfully completed maps */
		JobID completedReducers;								/* successfully completed reduces */
};

#endif
