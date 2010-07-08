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
		void assignMapJob(Node* node, ChunkID cid_start, ChunkID cid_end, string fileIn); /* assign a map job to a node */
		void assignReduceJob(Node* node, PartID pid, string fileOut);	/* assign a reduce job to a node */
		void resubmitMapJob(Node* node, struct MapJob job);			/* resubmit a map job to a node */
		void resubmitReduceJob(Node* node, struct ReduceJob job);		/* resubmit a reduce job to a node */
		void updateMaximumMapNode();							/* loops through all nodes and updates the maximum map */
		void updateMaximumReduceNode();						/* loops through all nodes and updates the maximum reduces */
		void loadNodesFile(string fileName);						/* load node names from file */
		void sendAllMappersFinished();							/* send all maps finished to all nodes */
		void printMaps(map<string, Node*> nodes); 				/* print and resubmit currently running maps */
		void printReduces(map<string, Node*> nodes);				/* print and resubmit currently running reduces */
		void broadcastKill();
		map<string, Node*> nodes;								/* maintain string list of all node names */
		Node* nodeWithMaxMapJobs;								/* node with maximum remaining map jobs */
		JobID maximumMapJobsCount;							/* the number of jobs assigned to highest map loaded node */
		Node* nodeWithMaxReduceJobs;							/* node with maximum remaining reduce jobs */
		JobID maximumReduceJobsCount;							/* the number of jobs assigned to highest reduce loaded node */
		vector<string> finishedNodes;							/* string list of finished node URL's */
		set<string> alreadySentFinishedNodes;						/* set of finished nodes we've sent out */
		JobID jidCounter;										/* monotonically increasing universal job counter */
		MapReduceSpecification* spec;								/* job specification parameters and constants */
		JobID activeMappers;									/* count of active mappers */
		JobID activeReducers;									/* count of active reducers */
		JobID remainingMappers;								/* remaining map jobs to run */
		JobID remainingReducers;								/* remaining reduce jobs to run */
		JobID completedMaps;									/* successfully completed maps */
		JobID completedReducers;								/* successfully completed reduces */
};

#endif
