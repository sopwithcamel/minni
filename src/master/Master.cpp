#include "Master.h"

Master::Master()
{
}

Master::~Master()
{
	vector<string*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		delete *nodesIter;
	}
}

void Master::loadNodesFile(string fileName)
{
	ifstream fileIn(fileName.c_str(), ifstream::in);
	string* temp;

	cout << "Reading Nodes File: " <<  fileName << endl;

	while (!fileIn.eof())
	{
		temp = new string();
		getline (fileIn,*temp);
		if (!temp->empty() && (*temp).compare("\n") && (*temp).compare(""))
		{
			nodes.push_back(temp);
			cout << "New Node:\t" << *temp << endl;
		}
	}
}

void Master::sendMapCommand()
{
	vector<string*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		cout << "Looping..." << endl;
		cout  << "Sending Map To: " << *(*nodesIter) << endl;

		TSocket* temp = new TSocket((*nodesIter)->c_str(), WORKER_PORT);
		boost::shared_ptr<TSocket> socket(temp);

		TBufferedTransport* temp1 = new TBufferedTransport(socket);
		boost::shared_ptr<TTransport> transport(temp1);

		TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
		boost::shared_ptr<TProtocol> protocol(temp2);

		WorkDaemonClient client(protocol);
		try {
			transport->open();
			client.startMapper(0,0);
			transport->close();
		} catch (TTransportException reason){
			cout << "Caught Exception: Sending MAP."  << endl;
		}
	}
}

void Master::sendReduceCommand()
{
	vector<string*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		cout << "Looping..." << endl;
		cout  << "Sending Map To: " << *(*nodesIter) << endl;

		TSocket* temp = new TSocket((*nodesIter)->c_str(), WORKER_PORT);
		boost::shared_ptr<TSocket> socket(temp);

		TBufferedTransport* temp1 = new TBufferedTransport(socket);
		boost::shared_ptr<TTransport> transport(temp1);

		TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
		boost::shared_ptr<TProtocol> protocol(temp2);

		WorkDaemonClient client(protocol);
		try {
			transport->open();
			client.startReducer(0,0,"/hdfs/tmp");
			transport->close();
		} catch (TTransportException reason){
			cout << "Caught Exception: Sending REDUCE." << endl;
		}
	}
}

void Master::checkStatus()
{
	vector<string*>::iterator nodesIter;
	for (nodesIter = nodes.begin() ; nodesIter < nodes.end(); nodesIter++ )
	{
		cout << "Looping..." << endl;
		cout  << "Sending Map To: " << *(*nodesIter) << endl;

		TSocket* temp = new TSocket((*nodesIter)->c_str(), WORKER_PORT);
		boost::shared_ptr<TSocket> socket(temp);

		TBufferedTransport* temp1 = new TBufferedTransport(socket);
		boost::shared_ptr<TTransport> transport(temp1);

		TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
		boost::shared_ptr<TProtocol> protocol(temp2);

		WorkDaemonClient client(protocol);
		try {
			transport->open();
			//client.pulse(std::map<JobID, Status>);
			transport->close();
		} catch (TTransportException reason){
			cout << "Caught Exception: Sending PULSE." << endl;
		}
	}
}
