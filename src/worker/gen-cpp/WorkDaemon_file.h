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

typedef spin_mutex Mutex;
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
// Concurrent
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

  void clear();

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
// Serial
class Transfer{
  private:
  Count total;
  Count progress;
  TransferStatus t_status;

  PartID pid;  
  Location location;
  /* This protects the progress, total and t_status members. This should 
   * be picked up whenever these are read or written. */
  Mutex mutex;

 public:
  Transfer(PartID p, Location l);
  TransferStatus getFile(const string& outfile); // Gets as much of the partition as possible
  TransferStatus checkStatus(); // Updates our knowledge of the partition
  string toString();
};

// Gets all of a partition; manages Transfers
// NB: this object is serial.
class PartitionGrabber{
 private:
  set<Location> locations;
  vector<Transfer> transfers;
  string outfile;
  PartID pid;
  bool finished;
  Mutex mutex;

 public:
  PartitionGrabber(PartID p = -1);
  PartStatus getStatus();
  void setPID(PartID p);
  void addLocation(Location l); // Adds a new location with data
  void addLocations(const vector<URL> u);
  void addLocations(const set<URL> u);
  void getMore(const string& outfile); // Gets as much of a partition as possible
  string toString();
  void reportDone();
};

typedef concurrent_hash_map<PartID, PartitionGrabber, 
  HashCompare<PartID> > GrabberMap;

//Concurrent Grabber managers
class GrabberRegistry{
 private:
  GrabberMap grab_map;
  set<URL> urls;
  bool finished;
  Mutex mutex;
  
  
 public:
  GrabberRegistry();
  void addLocations(const vector<URL> u);
  void setupGrabber(PartID p);
  void getMore(PartID pid, const string& outfile);
  PartStatus getStatus(PartID pid);
  void reportDone();
  string toString();
  void clear();
};

string local_file(PartID pid);
