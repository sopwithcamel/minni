#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"
#include "WorkDaemon_common.h"


// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"
#include "tbb/spin_mutex.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace workdaemon;
using namespace tbb;
using namespace std;

typedef unsigned long Length;
struct File{
  Length length;
  Count blocks;
  string name;
  PartID pid;
  JobID jid;
    File(JobID j=-1, PartID p=-1, string n="NOT A FILE");
  string toString() const;

  friend bool operator<(File const& lhs, File const& rhs)
  {
    return lhs.name.compare(rhs.name) < 0;
  }

  static const Length block_size; 
};


typedef concurrent_hash_map<PartID, vector<File>, HashCompare<PartID> > FileMap;
typedef concurrent_hash_map<PartID, set<string>, HashCompare<PartID> > NameMap;

// Tracks the status of the local files
class LocalFileRegistry{
 private:
  FileMap file_map;
  NameMap name_map;
  
 public:
  LocalFileRegistry();

  void bufferData(string &_return, 
		  const PartID _pid, const BlockID _bid); // Buffer chuck of data
  Count blocks(const PartID p) const; // Size of the partition so far
 
  // Report that jobs are complete
  void recordComplete(const JobID j, const PartID p, const string n);
  void recordComplete(const vector<File> &files);

  string toString() const;
};


// Some entity on the network
struct Location{
  string ip;
  unsigned int port;

  friend bool operator<(Location const& lhs, Location const& rhs)
  {
    return make_pair(lhs.ip, lhs.port) < make_pair(rhs.ip, rhs.port);
  }

};

// Transfers remote file from a single entity
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
  Status getFile(); // Gets as much of the partition as possible
  Status checkStatus(); // Updates our knowledge of the partition
  string toString();
};

// Gets all of a partition; manages Transfers
class PartitionGrabber{
 private:
  set<Location> locations;
  vector<Transfer> transfers;
  string outfile;
  PartID pid;

 public:
  PartitionGrabber(PartID p = -1, string o ="");
  void setValues(PartID p, string o);
  void addLocation(Location l); // Adds a new location with data
  void addLocations(const vector<URL> u);
  void addLocations(const set<URL> u);
  void getMore(); // Gets as much of a partition as possible
  string toString();
};

typedef concurrent_hash_map<PartID, PartitionGrabber, 
  HashCompare<PartID> > GrabberMap;
typedef spin_mutex Mutex;

class GrabberRegistry{
 private:
  GrabberMap grab_map;
  set<URL> urls;
  Mutex mutex;
  
  
 public:
  GrabberRegistry();
  void addLocations(const vector<URL> u);
  void getMore(PartID pid);
  string toString();
};

string local_file(PartID pid);
