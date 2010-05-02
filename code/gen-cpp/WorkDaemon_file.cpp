#include "WorkDaemon_file.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <cmath>

// File

const Length File::block_size = 10;

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

// File Registry implementation
LocalFileRegistry::LocalFileRegistry(){}


// Buffers some data as descibed by the pid and a block ID
void LocalFileRegistry::bufferData(string &_return, const PartID pid, const BlockID bid){
  // Get the right partition
  FileMap::accessor acc_file;
  bool found = file_map.find(acc_file, pid);
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
    cout << "start = " << start << ", len = " << len << endl;
    ifstream file(it->name.c_str(), ios::in);
    assert(file.is_open());
    // Read the block
    char memblock[File::block_size + 1];
    file.seekg(start, ios_base::beg);
    file.read(memblock, len);
    file.close();
    memblock[len] = '\0';
    _return.assign(memblock);
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


Transfer::Transfer(PartID p, Location l, string o): 
  pid(p), location(l), outfile(o),
  t_status(transferstatus::DNE), progress(0), total(-1){
  ofstream clearing(o.c_str(), ios::trunc|ios::out);
  clearing.close();
}

// Get as much of a file as we know about
Status Transfer::getFile(){
  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);

  // Get the next block and append.
  cout << "Fetching..."  << endl;
  string buffer;
  fstream fs (outfile.c_str(), fstream::app | fstream::out);
    assert(!fs.fail());
  transport->open();
  for(;progress < total;progress++){
    string buffer;
    client.sendData(buffer, pid, progress);
    cout << "...Caught..."  << endl;
    fs << buffer;
  }
  transport->close();
  fs.close();
}

// Update what we know about a file
Status Transfer::checkStatus(){
  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);

  // We can't become undone
  if(t_status == transferstatus::DONE){
    return t_status;
  }

  // Check for new blocks. Check for status first, because 
  transport->open();
  Status part_status = client.partitionStatus(pid);
  total = client.blockCount(pid); // update count
  transport->close();

  // Don't have any new ones
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
  t_status = transferstatus::READY;
  return t_status;
}

string Transfer::toString(){
  stringstream ss;

  ss << "L(" << location.ip << ":" << location.port << ") "
     << "(" << progress << "/" << total << ") " << t_status;
  
  return ss.str();
}


PartitionGrabber::PartitionGrabber(PartID p, string o): pid(p), outfile(o){}

// Add a new node to what we think is done
void PartitionGrabber::addLocation(Location l){
  if(locations.count(l) != 0){
    assert(locations.count(l) == 0);
    return;
  }
  transfers.push_back(Transfer(pid, l, outfile));
}

void PartitionGrabber::addLocations(vector<URL> l){
  for(vector<URL>::iterator it = l.begin();
      it != l.end(); it++){
    Location loc = {*it, WORKER_PORT};
    this->addLocation(loc);
  }
}

// Grab as much as we can from whatever we think is done.
void PartitionGrabber::getMore(){
  for(vector<Transfer>::iterator it = transfers.begin();
	it != transfers.end(); it++){
    Status status = it->checkStatus();
    if(status == transferstatus::READY){
      it->getFile();
    }
  }
}

string PartitionGrabber::toString(){
  stringstream ss;
  ss << "[" << endl;
  for(vector<Transfer>::iterator it = transfers.begin();
      it != transfers.end(); it++){
    ss << "\t" << it->toString() << endl;
  }
  ss << "]" << endl;
  return ss.str();
}
