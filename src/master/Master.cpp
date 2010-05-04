#include "Master.h"

Master::Master(MapReduceSpecification spec, DFS dfs, string nodesFile) : spec(spec), jidCounter(0), mappers(0), reducers(0), maximumMapJobsCount(UINT64_MIN), maximumReduceJobsCount(UINT64_MIN)
{
	loadNodesFile(nodesFile.c_str());
	vector<string*>::iterator iter;
	dfs.connect();

	/* assign all map jobs */
	for (iter = spec.getInputPath().begin(); iter < spec.getInputPath().end(); iter++)
	{
		uint64_t num_chunks = dfs.getNumChunks(*(*iter));
		cout << "Inspecting input file: " << *(*iter) << endl;
		for (uint64_t i = 0; i < num_chunks; i++)
		{
			vector<string> locations;
			Node* min;
			JobID minNumJobs = UINT64_MAX;
			vector<string>::iterator location_iter;
			for (location_iter = locations.begin(); location_iter < locations.end(); location_iter++)
			{
				cout << "Node[" << *location_iter << " has chunk " << i << andl;
				if (nodes[*location_iter].getNumRemainingJobs() < minNumJobs)
				{
					cout << "New min found: " << nodes[*location_iter].getNumRemainingJobs() << endl;
					minNumJobs  = nodes[*location_iter].getNumRemainingJobs();
					min = nodes[*location_iter];
				}
			}
			assignMapJob(min, i, *(*iter));
		}
	}
	dfs.disconnect();
	
	/* assign all reduce jobs */
	PartitionID numSingleNode = (PartitionID) ceil((double)spec.getNumReduces() / (double)nodes.size());
	PartitionID currentPID = 0;
	map<string, Node*>::iterator nodeIter;
	for (nodeIter = nodes.begin(); nodeIter != nodes.end(); nodeIter++)
	{
		for (PartitionID i = 0; i < numSingleNode; i++)
		{
			assignReduceJob(*nodeIter, currentPID + i, spec.getOutputPath());
			currentPID++;
		}
	}
	updateMaximumMapNode();
}

Master::~Master()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		delete ((*nodesIter).second);
	}
}

void Master::assignMapJob(Node* node, ChunkID cid, string fileIn)
{
	struct MapJob job(jidCounter*2, cid, jobstatus::INPROGRESS, fileIn, spec.getSoName(), spec.getMaxReduces(), spec.getDfsMaster(), spec.getDfsPort());
	node->addMap(job);
	remainingMappers++;
	jidCounter++;
}

void Master::assignReduceJob(Node* node, PartitionID pid, string fileOut)
{
	struct ReduceJob job(jidCounter*2+1, pid, jobstatus::INPROGRESS, fileOut, spec.getSoName(), spec.getDfsMaster(), spec.getDfsPort());
	node->addReduce(job);
	remainingReducers++;
	jidCounter++;
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
			nodes["test"] = new Node(tempURL, spec);
			cout << "New Node:\t" << *tempURL << endl;
		}
	}
}

void Master::assignMaps()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		cout <<  "Before Running Map Job Status: " << activeMappers << " active jobs " << remainingMappers << " remaining jobs" << endl; 
		/* decrement and increment counters accordingly */
		while ((*nodeIter).second->runningJobs() < spec.getMaxJobsPerNode())
		{
			if (!(*nodeIter).second->.runMap())
			{
				break;
			}
			activeMappers++;
			remainingMappers--;
		}

		updateMaximumMapNode();

		while ((*nodeIter).second->runningJobs() < spec.getMaxJobsPerNode())
		{
			struct MapJob job = nodeWithMaxMapJobs->stealMap();
			(*nodeIter).second->addMap(job);
			(*nodeIter).second->runMap();
			activeMappers++;
			remainingMappers--;
			updateMaximumMapNode();
		}

		cout <<  "After Running Map Job Status: " << activeMappers << " active jobs " << remainingMappers << " remaining jobs" << endl; 
	}
}

void Master::assignReduces()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		if ((*nodesIter)->hasMaps() || nodeWithMaxMapJobs->hasMaps()) return; /* assign all maps first */
		cout <<  "Before Running Reduce Job Status: " << activeMappers << " active jobs " << remainingMappers << " remaining jobs" << endl; 
		/* decrement and increment counters accordingly */
		while ((*nodeIter).second->runningJobs() < spec.getMaxJobsPerNode())
		{
			if (!(*nodeIter).second->.runReduce())
			{
				break;
			}
			activeReducers++;
			remainingReducers--;
		}

		updateMaximumReduceNode();

		while ((*nodeIter).second->runningJobs() < spec.getMaxJobsPerNode())
		{
			struct ReduceJob job = nodeWithMaxReduceJobs->stealMap();
			(*nodeIter).second->addReduce(job);
			(*nodeIter).second->runReduce();
			activeReducers++;
			remainingreducers--;
			updateMaximumReduceNode();
		}

		cout <<  "After Running Reduce Job Status: " << activeMappers << " active jobs " << remainingMappers << " remaining jobs" << endl; 
	}
}

void Master::updateMaximumMapNode()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		if (((*nodesIter).second)->numRemainingMapJobs() > maximumMapJobsCount)
		{
			maximumMapJobsCount = ((*nodesIter).second)->numRemainingMapJobs();
			nodeWithMaxMapJobs = ((*nodesIter).second);
		}
	}
}

void Master::updateMaximumReduceNode()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		if (((*nodesIter).second)->numRemainingReduceJobs() > maximumReduceJobsCount)
		{
			maximumReduceJobsCount = ((*nodesIter).second)->numRemainingReduceJobs();
			nodeWithMaxReduceJobs = ((*nodesIter).second);
		}
	}	
}

bool Master::checkMapStatus()
{
	if (remainingMappers) return true;
	return false;
}

bool Master::checkReduceStatus()
{
	if (remainingReducers) return true;
	return false;
}

bool Master::maps()
{
	if (activeMappers || remainingMappers) return true;
	return false;
}

bool Master::reduces()
{
	if (activeReducers || remainingReducers) return true;
	return false;
}

bool Master::checkStatus()
{
	map<string, Node*>::iterator nodesIter;
	map<JobID, Status> _return;
	map<JobID, Status>::iterator mapIterator;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		((*nodesIter).second)->checkStatus(_return);
		/* loop thru, add to finished list if finished with a map-phase */
		
		if (_return.begin() == _return.end())
		{
			cout << "No status to report from " <<  *(((*nodesIter).second)->getURL()) << endl;
			continue;
		}

		for ( mapIterator = _return.begin(); mapIterator != _return.end(); mapIterator++ )
		{
			cout << (*mapIterator).first << " => " << (*mapIterator).second << endl;
			if ((*mapIterator).first % 2 == 0) /* map */
			{
				if ((*mapIterator).second == jobstatus::DONE)
				{
					activeMappers--;
					completedMappers++;
					if (activeMappers == 0 && remainingMappers == 0)
					{
						sendAllMappersFinished();
					}
					finishedNodes.push_back(*(((*nodesIter).second)->getURL()));
					if (((*nodesIter).second)->removeMap((*mapIterator).first))
					{
						cout << "NODE[" << *(((*nodesIter).second)->getURL()) << "] FINISHED ALL MAPS!" << endl;
					}
					else
					{
						cout << "NODE[" << *(((*nodesIter).second)->getURL()) << "] still has MAPS!" << endl;
					}
				}
				else
				{
					cout << "Got MAP NOT DONE(" << (*mapIterator).second <<  ") job status from " << *(((*nodesIter).second)->getURL()) << endl;
				}
			}
			else /* reduce */
			{
				if ((*mapIterator).second == jobstatus::DONE)
				{
					activeReducers--;
					completedReducers++;
					if (((*nodesIter).second)->removeReduce((*mapIterator).first))
					{
						cout << "NODE[" << *(((*nodesIter).second)->getURL()) << "] FINISHED ALL REDUCES!" << endl;
						if (reducers == 0)
						{
							cout << "All reducers have now finished" << endl;
							sendFinishedNodes();
							return true;
						}
					}
				}
				else
				{
					cout << "Got MAP NOT DONE(" << (*mapIterator).second <<  ") job status from " << *((*nodesIter).second)->getURL() << endl;
				}
			}
		}
	}
	sendFinishedNodes();
	return false;
}

void Master::sendAllMappersFinished()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		((*nodesIter).second)->sendAllMapsFinished();
	}
}

void Master::sendFinishedNodes()
{
	/* check for unique new nodes that have finished a map task */
	vector<string>::iterator finishedIter;
	for (finishedIter = finishedNodes.begin(); finishedIter < finishedNodes.end(); finishedIter++)
	{
		if (alreadySentFinishedNodes.find(*finishedIter) != alreadySentFinishedNodes.end())
		{
			alreadySentFinishedNodes.insert(*finishedIter);
		}
		else
		{
			finishedNodes.erase(finishedIter);
		}
	}

	/* send new nodes that have finished a map task */
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		((*nodesIter).second)->reportCompletedJobs(finishedNodes);
	}
}

JobID Master::getNumberOfNodes()
{
	return nodes.size();
}

JobID Master::getNumberOfMapsCompleted()
{
	return completedMaps;
}

JobID Master::getNumberOfReducesCompleted()
{
	return completedReducers;
}

