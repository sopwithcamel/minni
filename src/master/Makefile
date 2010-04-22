all:
	g++ -Wall -I/usr/local/include/thrift gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Something.cpp gen-cpp/client.cpp -lthrift -o client
	g++ -Wall -I/usr/local/include/thrift gen-cpp/master_types.cpp gen-cpp/master_constants.cpp gen-cpp/Something.cpp gen-cpp/Something_server.cpp -lthrift -o server

clean:
	@rm client
	@rm server
