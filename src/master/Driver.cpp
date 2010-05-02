#include "Master.h"

int main(int argc, char* args[])
{
	Master m;
	m.loadNodesFile("example/nodes.conf");
	m.sendMapCommand();
	m.sendReduceCommand();
	m.checkStatus();
	return 0;
}
