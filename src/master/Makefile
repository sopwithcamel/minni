all:
	#thrift --gen cpp master.thrift
	#g++ -Wall -I/usr/local/include/thrift gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Something.cpp gen-cpp/client.cpp -lthrift -o client
	#g++ -Wall -I/usr/local/include/thrift gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Something.cpp gen-cpp/Something_server.cpp -lthrift -o server
	g++ hdfsConnector.cpp  -I/home/wolf/Dropbox/CMU/hadoop-0.20.2/src/c++/libhdfs -L/home/wolf/Dropbox/CMU/hadoop-0.20.2/c++/Linux-amd64-64/lib -lhdfs -o hdfs

clean:
	@rm client
	@rm server
	@rm -r gen-cpp
	@rm hdfs
