#include "Master.h"
#include <unistd.h>

#define LAG 5

int main(int argc, char* args[])
{
	Master m;
	m.loadNodesFile("example/nodes.conf");

	m.sendMapCommand();
	sleep(LAG);
	m.checkStatus();
	sleep(LAG);
	m.sendFinishedNodes();
	sleep(LAG);
	m.sendReduceCommand();
	sleep(LAG);
	m.checkStatus();
	sleep(LAG);
	return 0;
}
