#include "common.h"
#include "WorkDaemon_file.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <cmath>

/*********
***File***
**********/

const Length File::block_size = 4096;

File::File(JobID j, PartID p, string n): 
  jid(j), pid(p), name(n){
  ifstream file (this->name.c_str(), ios::in|ios::ate);
  assert(file.is_open());
  this->length = file.tellg();
  blocks =  ceil((float)this->length / (float)File::block_size);
}

string File::toString() const{
  stringstream ss;
  ss << "File[j:" << jid << ",p:" << pid << ",n:" << name << ",l:" << length << ",b:" << blocks << "]";
  return ss.str();
}

/**********************
***LocalFileRegistry***
***********************/

// File Registry implementation
LocalFileRegistry::LocalFileRegistry(){}


// Buffers some data as descibed by the pid and a block ID
void LocalFileRegistry::bufferData(string &_return, const PartID pid, const BlockID bid){
  // Get the right partition
  FileMap::accessor acc_file;
  bool found = file_map.find(acc_file, pid);
  if (!found)
    cout << "Asking for pid: " << pid << "and bid: " << bid << endl;
  assert(found);

  Count sum = 0;
  // Find the right file.
  for(vector<File>::iterator it = acc_file->second.begin(); it != acc_file->second.end(); it++){

    // Skip files until we land in the right one
    if(sum + it->blocks <= bid){
      sum += it->blocks;
      continue;
    }

    // This is the right file, scan the next block    
    Count start = (bid - sum) * File::block_size;
    Count len = min(File::block_size, it->length - start);
//    cout << "start = " << start << ", len = " << len << endl;
    ifstream file(it->name.c_str(), ios::in | ios::binary);
    assert(file.is_open());
    // Read the block
    char memblock[File::block_size + 1];
    file.seekg(start, ios_base::beg);
    file.read(memblock, len);
    file.close();
    _return.assign(memblock, len);
    return;
  }
}

// Checks the number of blocks in the partition
Count LocalFileRegistry::blocks(const PartID p) const{
  FileMap::accessor acc_part;
  bool found = file_map.find(acc_part ,p);
  if(!found){
    return 0;
  }
  
  Count sum = 0;
  for(vector<File>::const_iterator it = acc_part->second.begin();
      it != acc_part->second.end(); it++){
    sum += it->blocks;
  }
  return sum;
}

// Report a new complete file
void LocalFileRegistry::recordComplete(const JobID j, const PartID p, const string n){
  NameMap::accessor acc_name;
  this->name_map.insert(acc_name,p);
  if(acc_name->second.count(n) != 0){
    assert(acc_name->second.count(n) == 0);
    return;
  }
  
  FileMap::accessor acc_part;
  this->file_map.insert(acc_part, p);
  acc_part->second.push_back(File(j,p,n));
  acc_name->second.insert(n);
}

// Report a bunch of new files (i.e. a job with a number of partitions)
void LocalFileRegistry::recordComplete(const vector<File> &files){
  for(vector<File>::const_iterator it = files.begin(); it != files.end(); it++){
    this->recordComplete(it->jid, it->pid, it->name);
  }
}

string LocalFileRegistry::toString() const{
  stringstream ss;
  FileMap::const_range_type R = this->file_map.range();
  ss << "[" << endl;
  for(FileMap::const_iterator it = R.begin(); it != R.end(); it++){
    ss << "\t" << it->first << " -> [" << endl;
    for(vector<File>::const_iterator inner_it = it->second.begin();
	inner_it != it->second.end(); inner_it++){
      ss << "\t\t" << inner_it->toString() << endl;
      }
    ss << "\t]" << endl;
  }

  ss << "]" << endl;

  return ss.str();
}

void LocalFileRegistry::clear(){
  file_map.clear();
  name_map.clear();
}


/**************
 ***TRANSFER***
 **************/

/*
Notes: PartitionGrabber should be responsible for all 
*/

Transfer::Transfer(PartID p, Location l): 
  pid(p), location(l),
  t_status(transferstatus::DNE), progress(0), total(0){
}


// Get as much of a file as we know about
TransferStatus Transfer::getFile(const string& outfile){
  /* pick up mutex on Transfer object. This protects the progress, total and
   * t_status members. This should be picked up whenever these are read or
   * written. */
  Mutex::scoped_lock lock(mutex);

  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);

  // Get the next block and append.
  cout << "Fetching up to "  << total << "; current: " << progress << " from " << location.ip << endl;
  string buffer;
  fstream fs (outfile.c_str(), fstream::app | fstream::out | fstream::binary);
    assert(!fs.fail());
  transport->open();
  for(;progress < total;progress++){
    string buffer;
    client.sendData(buffer, pid, progress);
//    cout << "...Caught..."  << endl;
    fs << buffer;
  }
  cout << "Fetched: " << progress << " from " << location.ip << endl;
  transport->close();
  fs.close();
}

/* Statuses:
   Get the total number of blocks on the remote entity (actively check)
   BLOCKED -> progress == total
   DONE -> progress == total AND all mappers are done
   READY -> progress < total
 */
TransferStatus Transfer::checkStatus(){
  // pick up mutex on Transfer object
  Mutex::scoped_lock lock(mutex);

  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  workdaemon::WorkDaemonClient client(protocol);

  // Check for new blocks. Check for status first, because 
  transport->open();
  PartStatus part_status = client.mapperStatus();
  total = client.blockCount(pid); // update count
  transport->close();

  // Don't have any new ones
  assert(total >= progress);
  if(progress == total){
    // Was this because we're done or not?
    if(part_status == partstatus::DONE){
      t_status = transferstatus::DONE;
    }
    else{
      t_status = transferstatus::BLOCKED;
    }
    return t_status;
  }

  // There were some new ones!
  cout << "Transfer " << pid << " at " << location.ip << " is ready" << "(" << progress << ", " << total << ")" << endl;
  t_status = transferstatus::READY;
  return t_status;
}

/* Ok to not pick up mutex, because we're fine with the progress
 * being a little outdated */
string Transfer::toString(){
  stringstream ss;

  ss << "L(" << location.ip << ":" << location.port << ") "
     << "(" << progress << "/" << total << ") " << t_status;
  
  return ss.str();
}

/*********************
***PartitionGrabber***
**********************/


PartitionGrabber::PartitionGrabber(PartID p): 
		pid(p),
		finished(false)
{}

// Set the output file and the PartID
void PartitionGrabber::setPID(PartID p)
{
  pid = p;
}

/* Add a new node to what we think is done. Only called from 
 * addLocations() where we pick up the mutex */
void PartitionGrabber::addLocation(Location l)
{
  if(locations.count(l) == 0){
      locations.insert(l);
  }
  cout << "Inserting pid: " << pid << " for ip " << l.ip << endl;
  transfers.push_back(Transfer(pid, l));
}

void PartitionGrabber::addLocations(const vector<URL> l)
{
  // pick up PartitionGrabber object mutex
  Mutex::scoped_lock lock(mutex);

  for(vector<URL>::const_iterator it = l.begin();
      it != l.end(); it++){
    Location loc = {*it, WORKER_PORT};
    this->addLocation(loc);
  }
}

void PartitionGrabber::addLocations(const set<URL> l)
{
  // pick up PartitionGrabber object mutex
  Mutex::scoped_lock lock(mutex);

  for(set<URL>::const_iterator it = l.begin();
      it != l.end(); it++){
    Location loc = {*it, WORKER_PORT};
    this->addLocation(loc);
  }
}

// Grab as much as we can from whatever we think is done.
void PartitionGrabber::getMore(const string& outfile)
{
  // pick up PartitionGrabber object mutex
  Mutex::scoped_lock lock(mutex);

  for(vector<Transfer>::iterator it = transfers.begin();
	it != transfers.end(); it++){
    TransferStatus status = it->checkStatus();
    if(status == transferstatus::READY){
      it->getFile(outfile);
    }
  }
}

/*
  Status:
  If there is at least one transfer that is READY then the partition is READY
  If there are no transfers that are READY then the parition is BLOCKED
*/
PartStatus PartitionGrabber::getStatus()
{
  // pick up PartitionGrabber object mutex
  Mutex::scoped_lock lock(mutex);

  for(vector<Transfer>::iterator it = transfers.begin();
      it != transfers.end(); it++){
    if(it->checkStatus() == transferstatus::READY){
      return partstatus::READY;
    }
  }
  if (!finished)
    return partstatus::BLOCKED;
  else
    return partstatus::DONE;
}

string PartitionGrabber::toString()
{
  stringstream ss;
  ss << "Location = {";
  bool first = true;
  for(set<Location>::iterator it = locations.begin();
      it != locations.end(); it++){
    if(!first){
      ss << ", ";
    }
    ss << it->ip;
    first = false;
  }
  ss << "}" << endl;
  ss << "Trans[" << endl;
  for(vector<Transfer>::iterator it = transfers.begin();
      it != transfers.end(); it++){
    ss << "\t" << it->toString() << endl;
  }
  ss << "]" << endl;
  return ss.str();
}

void PartitionGrabber::reportDone()
{
  // pick up PartitionGrabber object mutex
  Mutex::scoped_lock lock(mutex);
  finished = true;
}

/********************
***GrabberRegistry***
*********************/

// Initially, the Grabber isn't done.
GrabberRegistry::GrabberRegistry()
{
  finished = false;
}

void GrabberRegistry::addLocations(const vector<URL> u)
{
  // pick up GrabberRegistry object mutex
  Mutex::scoped_lock lock(mutex);

  urls.insert(u.begin(), u.end()); // Add it to the master list
  GrabberMap::range_type range = grab_map.range();
  for(GrabberMap::iterator it = range.begin();
      it != range.end(); it++){
    it->second.addLocations(u); // Update any of the running grabbers
  }
}

void GrabberRegistry::getMore(PartID pid, const string& outfile)
{
  GrabberMap::accessor acc_grab;
  bool found = grab_map.find(acc_grab, pid);
  assert(found); // Can't get more if we don't have any! That's deep, Erik.
  acc_grab->second.getMore(outfile);
}

// Sets up a particular grabber, assigns an output file
void GrabberRegistry::setupGrabber(PartID p)
{
  GrabberMap::accessor acc_grab;
  grab_map.insert(acc_grab,p);
  acc_grab->second.setPID(p);
  acc_grab->second.addLocations(urls);
}

/*
What are the status messages:
DNE -> Cannot find the partition requested, new
READY -> PartitionGrabber thinks that it is available
BLOCKED -> PartitionGrabber thinks that it is blocked
DONE -> The master told us that all of the mappers finished, and the
 PartitionGrabber thinks that it is blocked
*/

PartStatus GrabberRegistry::getStatus(PartID pid)
{
  GrabberMap::accessor acc_grab;
  bool found = grab_map.find(acc_grab, pid);
  if(!found){
    return partstatus::DNE;
  }
  return acc_grab->second.getStatus();
}

// For the master to call when all Mappers are done.
void GrabberRegistry::reportDone()
{
  Mutex::scoped_lock lock(mutex);
  finished = true;
  GrabberMap::range_type range = grab_map.range();
  for(GrabberMap::iterator it = range.begin();
        it != range.end(); it++){
    it->second.reportDone();
  }
}

string GrabberRegistry::toString()
{
  stringstream ss;
  bool first = true;
  ss << "Finished = " << finished << endl;
  ss << "URLs = {";
  for(set<URL>::iterator it = urls.begin(); it != urls.end(); it++){
    if(!first){
      ss << ", ";
    }
    ss << *it;
    first = false;
  }
  ss << "}" << endl;
  GrabberMap::range_type R = grab_map.range();
  for(GrabberMap::iterator it = R.begin(); it != R.end(); it++){
    ss << "\t" << it->first <<": "<< it->second.toString() << endl;
  }
  return ss.str();
}

string local_file(PartID pid)
{
  stringstream ss;
  ss << "localfile_" << pid;
  return ss.str();
}

void GrabberRegistry::clear()
{
  Mutex::scoped_lock lock(mutex);
  grab_map.clear();
  urls.clear();
  finished = false;
}
