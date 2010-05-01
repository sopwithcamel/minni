#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"
#include "WorkDaemon_tasks.h"

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace workdaemon;
using namespace tbb;
using namespace std;


// Statuses
namespace transferstatus {enum {DNE, READY, BLOCKED, DONE};}
namespace partstatus {enum {AVAILABLE, BLOCKED, DONE};}

typedef unsigned long Length;
struct File{
  Length length;
  Count blocks;
  string name;
  PartID pid;
  JobID jid;
    File(JobID j=-1, PartID p=-1, string n="NOT A FILE");
  string toString() const;

  static const Length block_size = 10; 
};


typedef concurrent_hash_map<PartID, vector<File>, HashCompare<PartID> > FileMap;

// Tracks the status of the local files
class LocalFileRegistry{
 private:
  FileMap file_map;
  
 public:
  LocalFileRegistry();
  void bufferData(string &_return, const PartID _pid, const BlockID _bid);
  Count blocks(const PartID p) const;
 
  // Job Maintain
  void recordComplete(const JobID j, const PartID p, const string n);
  void recordComplete(const vector<File> &files);

  //DEBUG
  string toString() const;
};


// Entity
struct Location{
  string ip;
  unsigned int port;
};

// Transfers remote file
class Transfer{
  private:
  Count total;
  Count progress;
  Status t_status;

  PartID pid;  
  Location location;
  string outfile;

 public:
  Transfer(PartID p, Location l, string o);
  Status getFile();
  Status checkStatus();
  string toString();
};
