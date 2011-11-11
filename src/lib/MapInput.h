#ifndef LIB_MAPINPUT_H
#define LIB_MAPINPUT_H

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <unistd.h>
#include <sstream>
#include "daemon_types.h"
#include "KDFS.h"

class MapperWrapperTask;
class MapInput {
  public:
	MapInput() {};
	~MapInput() {};
	string data_location;	
	uint16_t port;
	string master_name;
	virtual void ParseProperties(Properties* prop) = 0;
};

class ChunkInput : public MapInput {
  friend class MapperWrapperTask;
  public:
	ChunkInput();
	~ChunkInput();
	uint64_t key_value(char** str, ChunkID id);
	void ParseProperties(Properties* p);
	ChunkID chunk_id_start;
	ChunkID chunk_id_end;
};

class FileInput : public MapInput {
  friend class MapperWrapperTask;
  public:
	FileInput();
	~FileInput();
	void getFileNames(vector<string>& filn);
	void ParseProperties(Properties* p);
	uint64_t file_ind_beg;
	uint64_t file_ind_end;
};
#endif
