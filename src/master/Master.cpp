#include "Master.h"

Master::Master(MapReduceSpecification spec, DFS &dfs, string nodesFile) : spec(spec)
{
	jidCounter = 0;
	activeMappers = 0;
	activeReducers = 0;
	remainingMappers = 0;
	remainingReducers = 0;
	completedMaps = 0;
	completedReducers = 0;
	maximumMapJobsCount = 0;
	maximumReduceJobsCount = 0;

	loadNodesFile(nodesFile.c_str());
	vector<string*>::iterator iter;
	dfs.connect();

	/* assign all map jobs */
	for (iter = spec.getInputFiles().begin(); iter < spec.getInputFiles().end(); iter++)
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
				cout << "Node[" << *location_iter << " has chunk " << i << endl;
				if (nodes[*location_iter]->numRemainingJobs() < minNumJobs)
				{
					cout << "New min found: " << nodes[*location_iter]->numRemainingJobs() << endl;
					minNumJobs  = nodes[*location_iter]->numRemainingJobs();
					min = nodes[*location_iter];
				}
			}
			assignMapJob(min, i, *(*iter));
		}
	}
	dfs.disconnect();
	
	/* assign all reduce jobs */
	PartID numSingleNode = (PartID) ceil((double)spec.getMaxReduces() / (double)nodes.size());
	PartID currentPID = 0;
	map<string, Node*>::iterator nodeIter;
	for (nodeIter = nodes.begin(); nodeIter != nodes.end(); nodeIter++)
	{
		for (PartID i = 0; i < numSingleNode; i++)
		{
			assignReduceJob((*nodeIter).second, currentPID + i, spec.getOutputPath());
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

void Master::assignReduceJob(Node* node, PartID pid, string fileOut)
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
		while ((*nodesIter).second->numActiveJobs() < spec.getMaxJobsPerNode())
		{
			if (!((*nodesIter).second->runMap()))
			{
				break;
			}
			activeMappers++;
			remainingMappers--;
		}

		updateMaximumMapNode();

		while ((*nodesIter).second->numActiveJobs() < spec.getMaxJobsPerNode())
		{
			struct MapJob job = nodeWithMaxMapJobs->stealMap();
			(*nodesIter).second->addMap(job);
			(*nodesIter).second->runMap();
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
		if ((*nodesIter).second->hasMaps() || nodeWithMaxMapJobs->hasMaps()) return; /* assign all maps first */
		cout <<  "Before Running Reduce Job Status: " << activeMappers << " active jobs " << remainingMappers << " remaining jobs" << endl; 
		/* decrement and increment counters accordingly */
		while ((*nodesIter).second->numActiveJobs() < spec.getMaxJobsPerNode())
		{
			if (!(*nodesIter).second->runReduce())
			{
				break;
			}
			activeReducers++;
			remainingReducers--;
		}

		updateMaximumReduceNode();

		while ((*nodesIter).second->numActiveJobs() < spec.getMaxJobsPerNode())
		{
			struct ReduceJob job = nodeWithMaxReduceJobs->stealReduce();
			(*nodesIter).second->addReduce(job);
			(*nodesIter).second->runReduce();
			activeReducers++;
			remainingReducers--;
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

bool Master::checkReducerStatus()
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
					completedMaps++;
					if (activeMappers + remainingMappers == 0)
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
						if (remainingReducers + activeReducers == 0)
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

