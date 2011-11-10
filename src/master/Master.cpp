#include "Master.h"

void printInputFiles(vector<string> vec)
{
	cout << "\t\tVector Size: " << vec.size() << endl;
	for (unsigned int i = 0; i < vec.size(); i++)
	{
		cout << "\t\tVector String Input File***: '" << (vec[i]) << "'" << endl;
	}
	vector<string>::iterator it = vec.begin();
	for (; it < vec.end(); it++)
	{
		cout << "\t\tVector String Input File: '" << (*it) << "'" << endl;
	}
}

Master::Master(MapReduceSpecification* spec, DFS &dfs, string nodesFile) : spec(spec)
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
	nodeWithMaxMapJobs = NULL;
	nodeWithMaxReduceJobs = NULL;

	loadNodesFile(nodesFile.c_str());
	vector<string>::iterator iter;
	cout << "Master: Loaded nodes file." << endl;
	cout << "Master: Connecting to DFS." << endl;
	dfs.connect();
	cout << "Master: Connected." << endl;
	cout << "Number of input files " << spec->getInputFiles().size() <<  endl;
	vector<string> inputFiles = spec->getInputFiles();
	map<string, Node*>::iterator nodeIter;

	uint64_t num_work_units;

	/* assign all map jobs */
	for (iter = inputFiles.begin(); iter < inputFiles.end(); iter++)
	{
		assert(dfs.checkExistence(*iter));

		/* Check if path is a directory. If it is then we assume, for
		 * now, that the directory structure is flat, ie. there are no
		 * sub-directories. Also, work will be allocated amongst the
		 * mappers in units of files in this case */
		if (dfs.isDirectory((*iter).c_str())) {
			uint64_t tot_bytes;
			dfs.getDirSummary((*iter).c_str(), num_work_units, tot_bytes);
			splitDir(dfs, *iter);
		} else {
			splitFile(dfs, *iter);
		}
	}
	cout << "All maps assigned." << endl;
	dfs.disconnect();
	cout << "Disconnected from DFS." << endl;
	
	/* assign all reduce jobs */
	//vv (PartID) ceil((double)spec->getMaxReduces() / (double)nodes.size());
	PartID numSingleNode = 1; 
	PartID currentPID = 0;
	cout << "Assigning reduce jobs." << endl;
	for (nodeIter = nodes.begin(); nodeIter != nodes.end(); nodeIter++) {
		for (PartID i = 0; i < numSingleNode; i++) {
			assignReduceJob((*nodeIter).second, currentPID + i, 
					spec->getOutputPath());
		}
		currentPID += numSingleNode;
	}
	updateMaximumMapNode();
}

Master::~Master()
{
	broadcastKill();
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		delete ((*nodesIter).second);
	}
}

void Master::splitFile(DFS& dfs, const string& fil)
{
	map<string, Node*>::iterator nodeIter;
	uint64_t num_chunks = dfs.getNumChunks((fil));
	uint64_t rem_chunks = num_chunks;
	int rem_nodes = getNumberOfNodes();
	uint64_t cids_for_map = 0;

	for (uint64_t i = 0; i < num_chunks; i+=cids_for_map)
	{
		assert(rem_nodes);
		cids_for_map = (uint64_t) floor(((double)rem_chunks / 
				rem_nodes));
		rem_chunks -= cids_for_map;
		rem_nodes--;

		vector<string> locs;
		Node* min = (*(nodes.begin())).second;
		JobID minJobs = INT64_MAX;
		vector<string>::iterator loc_it;
		dfs.getChunkLocations(fil, i, locs);
		for (loc_it = locs.begin(); loc_it < locs.end(); loc_it++) {
			/* make sure node is found in nodes list */
			if (nodes.find(*loc_it) != nodes.end()) {
				if (nodes[*loc_it]->numRemainingJobs() < minJobs) {
					minJobs  = nodes[*loc_it]->numRemainingJobs();
					min = nodes[*loc_it];
				}
			}
		}
		/* for now assign only 1 map per node */
		// find a node that has none
		if (min->numRemainingJobs() > 0) {
			for (nodeIter = nodes.begin(); nodeIter != 
					nodes.end(); nodeIter++) {
				if (nodeIter->second->numRemainingMapJobs() == 0) {
					min = nodeIter->second;
					break;
				}
			}
		}
		if (i+cids_for_map-1 < num_chunks) {
			assignMapJob(min, i, i+cids_for_map-1, (fil));
		} else {
			assignMapJob(min, i, num_chunks-1, (fil));
		}
	}
	
}

void Master::splitDir(DFS& dfs, const string& dir)
{
	map<string, Node*>::iterator nodeIter;
	vector<string> fils;
	// TODO: Assumes diretory structure is flat
	assert(dfs.readDir(dir, fils) == 0);

	uint64_t rem_fils = fils.size();
	int rem_nodes = getNumberOfNodes();
	uint64_t fils_for_map = 0;
	uint64_t fil_begin_ind = 0;

	// TODO: for now we don't optimize for locality of data
	for (nodeIter = nodes.begin(); nodeIter != nodes.end(); nodeIter++) {
		assert(rem_nodes);
		fils_for_map = (uint64_t) floor(((double)rem_fils / 
				rem_nodes));
		rem_fils -= fils_for_map;
		rem_nodes--;

		assignMapJob(nodeIter->second, fil_begin_ind, fil_begin_ind + 
				fils_for_map, dir);
	}
	
}

void Master::assignMapJob(Node* node, ChunkID cid_start, ChunkID cid_end, string fileIn)
{
	struct MapJob job(jidCounter*2, cid_start, cid_end, jobstatus::INPROGRESS, fileIn, spec->getSoName(), spec->getMaxReduces(), spec->getDfsMaster(), spec->getDfsPort());
	node->addMap(job);
	remainingMappers++;
	jidCounter++;
}

void Master::resubmitMapJob(Node* node, struct MapJob job)
{
	node->removeMap(job.jid);
	node->addMap(job);
	remainingMappers++;
	updateMaximumMapNode();

}

void Master::assignReduceJob(Node* node, PartID pid, string fileOut)
{
	struct ReduceJob job(jidCounter*2+1, pid, jobstatus::INPROGRESS, fileOut, spec->getSoName(), spec->getDfsMaster(), spec->getDfsPort());
	node->addReduce(job);
	remainingReducers++;
	jidCounter++;
}

void Master::resubmitReduceJob(Node* node, struct ReduceJob job)
{
	node->removeReduce(job.jid);
	node->addReduce(job);
	remainingReducers++;
	updateMaximumReduceNode();
}

void Master::loadNodesFile(string fileName)
{
	ifstream fileIn(fileName.c_str(), ifstream::in);
	string* tempURL;

	cout << "Reading Nodes File: " <<  fileName << endl;

	while (!fileIn.eof())
	{
		tempURL = new string(); /* deleted in communicator destructor */
		getline (fileIn,*tempURL);
		if (!tempURL->empty() && tempURL->compare("\n") && tempURL->compare(""))
		{
			nodes[*tempURL] = new Node(tempURL); /* deleted in master destructor */
			cout << "New Node:\t" << *tempURL << endl;
		}
	}
	broadcastKill();
}

void Master::checkState()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		string ret;
		(((*nodesIter).second))->sendState(ret);
		cout << "\t--> State Debug: " << ret << endl;
	}	
}

void Master::broadcastKill()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		(((*nodesIter).second))->sendKill();
	}
}

void Master::assignMaps()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		cout <<  "Before Running Map Job Status: " << activeMappers + activeReducers << " active jobs " << remainingMappers + remainingReducers << " remaining jobs" << endl; 
		/* decrement and increment counters accordingly */
		while ((*nodesIter).second->numActiveJobs() < spec->getMaxJobsPerNode())
		{
			if (!((*nodesIter).second->runMap()))
			{
				cout << "Breaking out of loop in assignMaps()" << endl;
				break;
			}
			activeMappers++;
			remainingMappers--;
		}

		cout << "assignMaps() updating max map node" << endl;
		updateMaximumMapNode();

		cout << "looping to steal" << endl;
		if (nodeWithMaxMapJobs != NULL)
		{
			while ((*nodesIter).second->numActiveJobs() < spec->getMaxJobsPerNode() && nodeWithMaxMapJobs->numRemainingMapJobs() > 0)
			{
				struct MapJob job = nodeWithMaxMapJobs->stealMap();
				(*nodesIter).second->addMap(job);
				if ((*nodesIter).second->runMap())
				{
					activeMappers++;
					remainingMappers--;
				}
				updateMaximumMapNode();
			}
		}

		cout <<  "After Running Map Job Status: " << activeMappers + activeReducers << " active jobs " << remainingMappers + remainingReducers << " remaining jobs" << endl; 
	}
}

void Master::assignReduces()
{
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		if ((*nodesIter).second->hasMaps() || nodeWithMaxMapJobs->hasMaps()) return; /* assign all maps first */
		cout <<  "Before Running Reduce Job Status: " << activeMappers + activeReducers << " active jobs " << remainingMappers + remainingReducers << " remaining jobs" << endl; 
		/* decrement and increment counters accordingly */
		while ((*nodesIter).second->numActiveJobs() < spec->getMaxJobsPerNode())
		{
			if (!(*nodesIter).second->runReduce())
			{
				cout << "Breaking out of loop in assignReduces()" << endl;
				break;
			}
			activeReducers++;
			remainingReducers--;
		}

		updateMaximumReduceNode();

		cout << "Looping to steal reduce jobs" << endl;

		if (nodeWithMaxReduceJobs != NULL)
		{
			while ((*nodesIter).second->numActiveJobs() < spec->getMaxJobsPerNode() && nodeWithMaxReduceJobs->numRemainingReduceJobs() > 0)
			{
				cout << "Stealing a reduce job" << endl;
				struct ReduceJob job = nodeWithMaxReduceJobs->stealReduce();
				(*nodesIter).second->addReduce(job);
				if ((*nodesIter).second->runReduce())
				{
					activeReducers++;
					remainingReducers--;
				}
				updateMaximumReduceNode();
			}
		}

		cout <<  "After Running Reduce Job Status: " << activeMappers + activeReducers << " active jobs " << remainingMappers + remainingReducers << " remaining jobs" << endl; 
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
	cout << "nodeWithMaxMapJobs set to: " << nodeWithMaxMapJobs << endl;	
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
	cout << "nodeWithMaxReduceJobs set to: " << nodeWithMaxReduceJobs << endl;
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

void Master::printMaps(map<string, Node*> nodes)
{
	map<string, Node*>::iterator nodesIter;
	vector<struct MapJob*> _return;
	cout << "*** Printing active maps." << endl;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		((*nodesIter).second)->printActiveMaps(_return);
		vector<struct MapJob*>::iterator jobIter;
		for (jobIter = _return.begin(); jobIter < _return.end(); jobIter++)
		{
			(*jobIter)->staleness++;
			cout << "Job[" << (*jobIter)->jid << "] Staleness: " << (*jobIter)->staleness << endl;
			if ((*jobIter)->staleness >= 100000)
			{
				cout << "Resubmitting a map job: " << (*jobIter)->jid << endl;
				activeMappers--;
				(*jobIter)->staleness = 0;
				resubmitMapJob((*nodesIter).second, *(*jobIter));
			}
		}
	}
}

void Master::printReduces(map<string, Node*> nodes)
{
	map<string, Node*>::iterator nodesIter;
	vector<struct ReduceJob*> _return;
	cout << "*** Printing active reduces." << endl;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		((*nodesIter).second)->printActiveReduces(_return);
		vector<struct ReduceJob*>::iterator jobIter;
		for (jobIter = _return.begin(); jobIter < _return.end(); jobIter++)
		{
			(*jobIter)->staleness++;
			cout << "Job[" << (*jobIter)->jid << "] Staleness: " << (*jobIter)->staleness << endl;
			if ((*jobIter)->staleness >= 100000)
			{
				cout << "Resubmitting a reduce job: " << (*jobIter)->jid << endl;
				activeReducers--;
				(*jobIter)->staleness = 0;
				resubmitReduceJob((*nodesIter).second, *(*jobIter));
			}
		}
	}
}

bool Master::maps()
{
	cout << "Checking map jobs: "  << activeMappers << " active mappers " << remainingMappers << " remaining mappers." << endl;
	if (activeMappers + remainingMappers <= 5) printMaps(nodes);
	if (activeMappers || remainingMappers) return true;
	return false;
}

bool Master::reduces()
{
	cout << "Checking reduce jobs: "  << activeReducers << " active reducers " << remainingReducers << " remaining reducers." << endl;
	if (activeReducers + remainingReducers <= 5) printReduces(nodes);
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
			cout << "\t\t" << "Job[" << (*mapIterator).first << "] => Status[" << (*mapIterator).second << "]" << endl;
			if ((*mapIterator).first % 2 == 0) /* map */
			{
				if ((*mapIterator).second == jobstatus::DONE || ((*mapIterator).second == jobstatus::DEAD))
				{
					if (((*nodesIter).second)->removeMap((*mapIterator).first))
					{
						activeMappers--;
						completedMaps++;
						finishedNodes.push_back(*(((*nodesIter).second)->getURL()));
						cout << "Saying " << ((*nodesIter).second)->getURL() << " is done: " << activeMappers << ", " << completedMaps << endl;
					}
				}
				else
				{
					cout << "Got MAP NOT DONE(" << (*mapIterator).second <<  ") job status from " << *(((*nodesIter).second)->getURL()) << endl;
				}
			}
			else /* reduce */
			{
				if ((*mapIterator).second == jobstatus::DONE || (*mapIterator).second == jobstatus::DEAD)
				{
					if (((*nodesIter).second)->removeReduce((*mapIterator).first))
					{
						activeReducers--;
						completedReducers++;
						if (remainingReducers + activeReducers == 0)
						{
							cout << "All reducers have now finished" << endl;
							TimeLog::addTimeStamp("Master: All reducers done");
							TimeLog::dumpLog();
							sendFinishedNodes();
							return true;
						}
					}
				}
				else
				{
					cout << "Got REDUCE NOT DONE(" << (*mapIterator).second <<  ") job status from " << *((*nodesIter).second)->getURL() << endl;
				}
			}
		}
	}
	sendFinishedNodes();
	return false;
}

void Master::sendAllMappersFinished()
{
	static bool sent = false;
	if (!sent)
	{
		map<string, Node*>::iterator nodesIter;
		for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
		{
			((*nodesIter).second)->sendAllMapsFinished();
		}
		sent = true;
	}
}

void Master::sendFinishedNodes()
{
	/* check for unique new nodes that have finished a map task */
	vector<string>::iterator finishedIter;
	for (finishedIter = finishedNodes.begin(); finishedIter < finishedNodes.end(); finishedIter++)
	{
		cout << "more work on " << *finishedIter << endl;
		if (alreadySentFinishedNodes.find(*finishedIter) == alreadySentFinishedNodes.end())
		{
			alreadySentFinishedNodes.insert(*finishedIter);
		}
	}

	/* send new nodes that have finished a map task */
	map<string, Node*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter != nodes.end(); nodesIter++ )
	{
		((*nodesIter).second)->reportCompletedJobs(finishedNodes);
	}
	cout << "\t\terasing finishnodes" << endl;
	finishedNodes.clear();

	if (activeMappers + remainingMappers == 0)
	{
		sendAllMappersFinished();
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

