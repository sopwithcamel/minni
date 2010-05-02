#include "Master.h"

Master::Master()
{
}

Master::~Master()
{
	vector<Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		delete *nodesIter;
	}
}

void Master::loadNodesFile(string fileName)
{
	ifstream fileIn(fileName.c_str(), ifstream::in);
	string* tempURL;

	cout << "Reading Nodes File: " <<  fileName << endl;

	while (!fileIn.eof())
	{
		tempURL = new string();
		getline (fileIn,*tempURL);
		if (!tempURL->empty() && tempURL->compare("\n") && tempURL->compare(""))
		{
			nodes.push_back(new Node(tempURL));
			cout << "New Node:\t" << *tempURL << endl;
		}
	}
}

void Master::sendMapCommand()
{
	vector<Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		/* maps get even jid's */
		(*nodesIter)->addMap(jidCounter*2,0,"/tmp/fs");
		jidCounter++;
	}
}

void Master::sendReduceCommand()
{
	vector<Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		/* reduces get odd job id's */
		(*nodesIter)->addReduce((jidCounter*2) + 1,0,"/tmp/fs");
		jidCounter++;
	}
}

bool Master::checkStatus()
{
	vector<Node*>::iterator nodesIter;
	map<JobID, Status> _return;
	map<JobID, Status>::iterator mapIterator;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		(*nodesIter)->checkStatus(_return);
		/* loop thru, add to finished list if finished with a map-phase */
		for ( mapIterator=_return.begin() ; mapIterator != _return.end(); mapIterator++ )
		{
			cout << (*mapIterator).first << " => " << (*mapIterator).second << endl;
			if ((*mapIterator).first % 2 == 0) /* map */
			{
				if ((*mapIterator).second == jobstatus::DONE)
				{
					if ((*nodesIter)->removeMap((*mapIterator).first))
					{
						cout << "NODE[" << *((*nodesIter)->getURL()) << "] FINISHED ALL MAPS!" << endl;
						mappers--;
						finishedNodes.push_back(*((*nodesIter)->getURL()));
					}
				}
			}
			else /* reduce */
			{
				if ((*mapIterator).second == jobstatus::DONE)
				{
					if ((*nodesIter)->removeReduce((*mapIterator).first))
					{
						cout << "NODE[" << *((*nodesIter)->getURL()) << "] FINISHED ALL REDUCES!" << endl;
						reducers--;
						if (reducers == 0)
						{
							return true;
						}
					}
				}
			}
		}
	}
	return false;
}

void Master::sendFinishedNodes()
{
	vector<Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		(*nodesIter)->reportCompletedJobs(finishedNodes);
	}	
}
