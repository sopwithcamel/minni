all:
	g++ -Wall -I/usr/local/include/thrift gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Master.cpp gen-cpp/Master_Server.cpp -lthrift -o server

daemon:
	g++ -Wall -I/usr/local/include/thrift -I../../code/gen-cpp ../../code/gen-cpp/daemon_types.cpp ../../code/gen-cpp/daemon_constants.cpp ../../code/gen-cpp/WorkDaemon.cpp WorkDaemonClient.cpp -lthrift -o client

thrift:
	thrift --gen cpp master.thrift

hdfs:
	g++ hdfsConnector.cpp  -I/home/wolf/Dropbox/CMU/hadoop-0.20.2/src/c++/libhdfs -L/home/wolf/Dropbox/CMU/hadoop-0.20.2/c++/Linux-amd64-64/lib -lhdfs -o hdfs
		
clean:
	@rm client
	@rm server
	@rm -r gen-cpp
	@rm hdfs
