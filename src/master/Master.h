#ifndef MINNIE_MASTER_H
#define MINNIE_MASTER_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>

#define __STDC_LIMIT_MACROS
#include <stdint.h>

using namespace std;

#include "Communicator.h"
#include "Node.h"
#include "KDFS.h"
#include "Minnie.h"
#include "util.h"

class Master {
	public:
		Master(MapReduceSpecification* spec, DFS &dfs, string nodesFile);/* constructor */
		~Master();											/* destructor */
		void assignMaps();										/* assign and run as many maps as possible */
		void assignReduces();									/* assign and run as many reduces as possible */
		bool checkStatus();										/* poll all nodes for status */
		bool checkMapStatus();									/* check if all maps have completed */
		bool checkReducerStatus();								/* check if all reduces have completed */
		void checkState();										/* debug functionality */
		void sendFinishedNodes();								/* send list of finished URL's */
		bool maps();											/* check if map jobs still remain to be assigned */
		bool reduces();										/* check if reduce jobs still remain to be assigned */
		JobID getNumberOfNodes();								/* return number of nodes used */
		JobID getNumberOfMapsCompleted();						/* return number of maps finished */
		JobID getNumberOfReducesCompleted();					/* return number of reduces finished */
	private:
		/* assign a map job to a node */
		void assignMapJob(Node* node, ChunkID cid_start, ChunkID cid_end, 
				string fileIn);
		/* assign a reduce job to a node */
		void assignReduceJob(Node* node, PartID pid, string fileOut);
		/* resubmit a map job to a node */
		void resubmitMapJob(Node* node, struct MapJob job);
		/* resubmit a reduce job to a node */
		void resubmitReduceJob(Node* node, struct ReduceJob job);
		/* loops through all nodes and updates the maximum map */
		void updateMaximumMapNode();
		/* loops through all nodes and updates the maximum reduces */
		void updateMaximumReduceNode();
		/* load node names from file */
		void loadNodesFile(string fileName);
		/* send all maps finished to all nodes */
		void sendAllMappersFinished();
		/* print and resubmit currently running maps */
		void printMaps(map<string, Node*> nodes);
		/* print and resubmit currently running reduces */
		void printReduces(map<string, Node*> nodes);
		/* split chunks of large file among mappers */
		void splitFile(DFS&, const string&);
		/* split files in directory among mappers */
		void splitDir(DFS&, const string&);
		void broadcastKill();
		/* maintain string list of all node names */
		map<string, Node*> nodes;
		/* node with maximum remaining map jobs */
		Node* nodeWithMaxMapJobs;
		/* the number of jobs assigned to highest map loaded node */
		JobID maximumMapJobsCount;
		/* node with maximum remaining reduce jobs */
		Node* nodeWithMaxReduceJobs;
		/* the number of jobs assigned to highest reduce loaded node */
		JobID maximumReduceJobsCount;
		/* string list of finished node URL's */
		vector<string> finishedNodes;
		/* set of finished nodes we've sent out */
		set<string> alreadySentFinishedNodes;
		/* monotonically increasing universal job counter */
		JobID jidCounter;
		/* job specification parameters and constants */
		MapReduceSpecification* spec;
		/* count of active mappers */
		JobID activeMappers;
		/* count of active reducers */
		JobID activeReducers;
		/* remaining map jobs to run */
		JobID remainingMappers;
		/* remaining reduce jobs to run */
		JobID remainingReducers;
		/* successfully completed maps */
		JobID completedMaps;
		/* successfully completed reduces */
		JobID completedReducers;
};

#endif
