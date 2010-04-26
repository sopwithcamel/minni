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

using namespace workdaemon;
using namespace tbb;
using namespace std;

// Status
namespace JobStatus {enum {DNE, INPROGRESS, DONE, DONE_AND_REPORTED, DEAD, DEAD_AND_REPORTED};}


template <class T>
struct HashCompare {
  static size_t hash( const T& x){
    return (size_t) x;
  }
  static bool equal( const T& x, const T& y ){
    return x == y;
  }
};

typedef concurrent_hash_map<JobID,Status,HashCompare<JobID> > JobStatusMap;
