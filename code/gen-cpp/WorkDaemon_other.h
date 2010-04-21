#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"

// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

#define PARAM_M 20
#define PARAM_R 10
#define PARAM_FILEBUFFER 2000

// Task Status
#define TASK_INPROGRESS 0
#define TASK_COMPLETE 1
#define TASK_DEAD 2

// File Status
 
struct FileStatus{
  unsigned int status;
  unsigned int position;

  static const unsigned int DNE = 0;
  static const unsigned int READY = 1;
  static const unsigned int INPROGRESS = 2;
  static const unsigned int DONE = 3;
};

using namespace workdaemon;
using namespace tbb;
using namespace std;

template <class T> 
struct HashCompare { 
  static size_t hash( const T& x){
    return (size_t) x;
  }
  static bool equal( const T& x, const T& y ){
    return x == y;
  }
};

template <class T, class U>
  struct PairHashCompare{
    static size_t hash( const pair<T,U>& x){
      size_t seed = (size_t)x.first;
      return (size_t)x.second + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }
    static bool equal( const pair<T,U>& x, const pair<T,U>& y ){
      return x == y;
    }
  };


typedef concurrent_hash_map<PartitionID, 
  map<JobID, FileStatus>, HashCompare<PartitionID> > FileStatusMap;

class FileRegistry{
 private:
  FileStatusMap registry;
 public:
  void get_data(JobID jid, PartitionID pid, vector<vector<string> > &_return);
  Status get_status(JobID jid, ParitionID pid);
  void find_new(JobID jid, PartitionID pid);
};

string local_filename(JobID jid, PartitionID pid);
void generate_keyvalue_pairs(string filename, int num);

