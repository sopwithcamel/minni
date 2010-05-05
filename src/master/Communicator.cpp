#include "Communicator.h"

Communicator::~Communicator()
{
	delete url;
}

void Communicator::sendMap(struct MapJob map, uint16_t retries)
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
		if (retries > 0) sendMap(map, --retries);
	}
}

void Communicator::sendReduce(struct ReduceJob reduce, uint16_t retries)
{
	cout  << "Sending Reduce["<< reduce.jid << "] To: " << *url << endl;

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
		if (retries > 0) sendReduce(reduce, --retries);
	}
}

void Communicator::sendListStatus(std::map<JobID, Status> & _return, uint16_t retries)
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
		if (retries > 0) sendListStatus(_return, --retries);
	}
}

void Communicator::sendReportCompletedJobs(const std::vector<URL> & done, uint16_t retries)
{
	if (done.size() == 0) return;
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
		if (retries > 0) sendReportCompletedJobs(done, --retries);
	}
}

void Communicator::sendAllMapsDone(uint16_t retries)
{
	cout  << "Sending allMapsDone To: " << *url << endl;

	TSocket* temp = new TSocket(url->c_str(), WORKER_PORT);
	boost::shared_ptr<TSocket> socket(temp);

	TBufferedTransport* temp1 = new TBufferedTransport(socket);
	boost::shared_ptr<TTransport> transport(temp1);

	TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
	boost::shared_ptr<TProtocol> protocol(temp2);

	WorkDaemonClient client(protocol);

	try {
		transport->open();
		client.allMapsDone();
		transport->close();
	} catch (TTransportException reason){
		cout << "Caught Exception: Sending ALLMAPSDONE."  << endl;
		if (retries > 0) sendAllMapsDone(--retries);
	}
}

void Communicator::sendKill(uint16_t retries)
{
	cout  << "Sending kill To: " << *url << endl;

	TSocket* temp = new TSocket(url->c_str(), WORKER_PORT);
	boost::shared_ptr<TSocket> socket(temp);

	TBufferedTransport* temp1 = new TBufferedTransport(socket);
	boost::shared_ptr<TTransport> transport(temp1);

	TBinaryProtocol* temp2 = new TBinaryProtocol(transport);
	boost::shared_ptr<TProtocol> protocol(temp2);

	WorkDaemonClient client(protocol);

	try {
		transport->open();
		client.kill();
		transport->close();
	} catch (TTransportException reason){
		cout << "Caught Exception: Sending KILL."  << endl;
		if (retries > 0) sendAllMapsDone(--retries);
	}
}

string* Communicator::getURL()
{
	return url;
}
