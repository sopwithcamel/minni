#include "MapInput.h"

ChunkInput::ChunkInput()
{
}

ChunkInput::~ChunkInput()
{
}

uint64_t ChunkInput::key_value(char** str, ChunkID id)
{
	KDFS myhdfs(master_name,port);
	bool conn = myhdfs.connect();
	assert(conn); // Unable to establish connection :(

	// looking for file
	assert(myhdfs.checkExistence(data_location));
	uint64_t length = myhdfs.getChunkSize(data_location);

	// Going to read chunks from KDFS
	uint64_t offset = id*length;
	int64_t k = myhdfs.readChunkOffset(data_location, offset, *str, length);
	assert(k != -1);
	cout<<"Mapper: Read " << k << " blocks" << endl;

	myhdfs.closeFile(data_location);
	bool disconn = myhdfs.disconnect();
	assert(disconn);
	return k;
}

void ChunkInput::ParseProperties(Properties* p)
{
	stringstream ss;
  	data_location = (*p)["FILE_IN"];
	cout<<"Mapper: file location is "<<data_location<<endl;
	string chunk_temp_start = (*p)["CID_START"];
	ss <<chunk_temp_start;
	ss >> chunk_id_start;
	string chunk_temp_end = (*p)["CID_END"];
	stringstream ss4;
	ss4 <<chunk_temp_end;
	ss4 >> chunk_id_end;
	cout<<"Mapper: chunk id  start is "<<chunk_id_start<<endl;
	cout<<"Mapper: chunk id end is "<<chunk_id_end<<endl;
  	master_name = (*p)["DFS_MASTER"];
	cout<<"Mapper: dfs master is "<<master_name<<endl;
        string port_temp = (*p)["DFS_PORT"];
	stringstream ss2;
	ss2 <<port_temp;
	uint16_t port_int;
	ss2 >> port_int;
	port =  port_int;
	cout<<"Mapper: port - the string version is "<<(*p)["DFS_PORT"]<<endl;
	cout<<"Mapper: port is -converted version "<<port<<endl;
}

FileInput::FileInput()
{
}

FileInput::~FileInput()
{
}

void FileInput::getFileNames(vector<string>& filn)
{
	KDFS dfs(master_name,port);
	bool conn = dfs.connect();
	assert(conn); // Unable to establish connection :(

	// looking for directory
	assert(dfs.checkExistence(data_location));
	dfs.readDir(data_location, filn);
	// remove . and ..
	filn.erase(filn.begin());
	filn.erase(filn.begin());

	dfs.closeFile(data_location);
	bool disconn = dfs.disconnect();
	assert(disconn);
}

void FileInput::ParseProperties(Properties* p)
{
	stringstream ss;
  	data_location = (*p)["FILE_IN"];
	cout<<"Mapper: file location is " << data_location << endl;
	string file_temp_begin = (*p)["CID_START"];
	ss << file_temp_begin;
	ss >> file_ind_beg;
	string file_temp_end = (*p)["CID_END"];
	stringstream ss4;
	ss4 << file_temp_end;
	ss4 >> file_ind_end;
	cout<<"Mapper: file id  start is " << file_ind_beg << endl;
	cout<<"Mapper: file id end is " << file_ind_end << endl;
  	master_name = (*p)["DFS_MASTER"];
	cout<<"Mapper: dfs master is " << master_name << endl;
        string port_temp = (*p)["DFS_PORT"];
	stringstream ss2;
	ss2 <<port_temp;
	uint16_t port_int;
	ss2 >> port_int;
	port =  port_int;
	cout<<"Mapper: port - the string version is "<<(*p)["DFS_PORT"]<<endl;
	cout<<"Mapper: port is -converted version "<<port<<endl;
}
