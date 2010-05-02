#pragma once

#include <iostream>
#include <sstream>
#include "WorkDaemon.h"
// TBB includes
#include "tbb/concurrent_hash_map.h"
#include "tbb/task.h"
#include "tbb/tbb_thread.h"

using namespace workdaemon;
using namespace tbb;
using namespace std;

#define WORKER_PORT 9090


// Task Registry
string printReport(map<JobID,Status> &M);
typedef map<JobID, Status> Report;

// Statuses
namespace transferstatus {enum {DNE, READY, BLOCKED, DONE};}
namespace partstatus {enum {AVAILABLE, BLOCKED, DONE};}
namespace jobstatus {enum {DNE, INPROGRESS, DONE, DONE_AND_REPORTED, DEAD, DEAD_AND_REPORTED};}
namespace jobkind {enum {NIL, MAPPER, REDUCER};}

typedef unsigned int JobKind;

template <class T>
struct HashCompare {
  static size_t hash( const T& x){
    return (size_t) x;
  }
  static bool equal( const T& x, const T& y ){
    return x == y;
  }
};
