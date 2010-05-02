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
		void checkStatus();					/* poll all nodes for status */
	private:
		vector<string*> nodes;			/* maintain string list of all node names */
};

#endif
