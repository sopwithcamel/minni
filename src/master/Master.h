#ifndef MINNIE_MASTER_H
#define MINNIE_MASTER_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

using namespace std;

#include "Communicator.h"
#include "DFS.h"
#include "Node.h"

class Master {
	public:
		Master();							/* constructor */
		~Master();						/* destructor */
		void loadNodesFile(string fileName);	/* load node names from file */
		void sendMapCommand();			/* sends map command to all nodes */
		void sendReduceCommand();			/* sends reduce command to all nodes */
		bool checkStatus();					/* poll all nodes for status */
		void sendFinishedNodes();			/* send list of finished URL's */
	private:
		vector<Node*> nodes;				/* maintain string list of all node names */
		vector<string> finishedNodes;		/* string list of finished node URL's */
		JobID jidCounter;					/* monotonically increasing universal job counter */
		int64_t mappers;					/* count of active mappers */
		int64_t reducers;					/* count of active reducers */
};

#endif
