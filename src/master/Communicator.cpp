#include "Communicator.h"

Communicator::~Communicator()
{
	delete url;
}

void Communicator::sendMap(MapJob map)
{
	cout  << "Sending Map To: " << *url << endl;

	TSocket* temp = new TSocket(url->c_str(), WORKER_PORT);
	boost::shared_ptr<TSocket> socket(temp);

	TBufferedTransport* temp1 = new TBufferedTransport(socket);
	boost::shared_ptr<TTransport> transport(temp1);

	TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
	boost::shared_ptr<TProtocol> protocol(temp2);

	WorkDaemonClient client(protocol);

	try {
		transport->open();
		client.startMapper(map.jid, map.prop);
		transport->close();
	} catch (TTransportException reason){
		cout << "Caught Exception: Sending MAP."  << endl;
	}
}

void Communicator::sendReduce(ReduceJob reduce)
{
	cout  << "Sending Reduce To: " << *url << endl;

	TSocket* temp = new TSocket(url->c_str(), WORKER_PORT);
	boost::shared_ptr<TSocket> socket(temp);

	TBufferedTransport* temp1 = new TBufferedTransport(socket);
	boost::shared_ptr<TTransport> transport(temp1);

	TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
	boost::shared_ptr<TProtocol> protocol(temp2);

	WorkDaemonClient client(protocol);

	try {
		transport->open();
		client.startReducer(reduce.jid, reduce.prop);
		transport->close();
	} catch (TTransportException reason){
		cout << "Caught Exception: Sending REDUCE."  << endl;
	}	
}

void Communicator::sendListStatus(std::map<JobID, Status> & _return)
{
	cout  << "Sending ListStatus To: " << *url << endl;

	TSocket* temp = new TSocket(url->c_str(), WORKER_PORT);
	boost::shared_ptr<TSocket> socket(temp);

	TBufferedTransport* temp1 = new TBufferedTransport(socket);
	boost::shared_ptr<TTransport> transport(temp1);

	TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
	boost::shared_ptr<TProtocol> protocol(temp2);

	WorkDaemonClient client(protocol);

	try {
		transport->open();
		client.listStatus(_return);
		transport->close();
	} catch (TTransportException reason){
		cout << "Caught Exception: Sending LISTSTATUS."  << endl;
	}
}

void Communicator::sendReportCompletedJobs(const std::vector<URL> & done)
{
	cout  << "Sending reportCompletedJobs To: " << *url << endl;

	TSocket* temp = new TSocket(url->c_str(), WORKER_PORT);
	boost::shared_ptr<TSocket> socket(temp);

	TBufferedTransport* temp1 = new TBufferedTransport(socket);
	boost::shared_ptr<TTransport> transport(temp1);

	TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
	boost::shared_ptr<TProtocol> protocol(temp2);

	WorkDaemonClient client(protocol);

	try {
		transport->open();
		client.reportCompletedJobs(done);
		transport->close();
	} catch (TTransportException reason){
		cout << "Caught Exception: Sending REPORTCOMPLETEDJOBS."  << endl;
	}
}

string* Communicator::getURL()
{
	return url;
}
