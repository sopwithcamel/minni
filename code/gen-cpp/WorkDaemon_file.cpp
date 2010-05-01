#include "WorkDaemon_file.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <cmath>

// File

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

void LocalFileRegistry::bufferData(string &_return, const PartID pid, const BlockID bid){
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
    // read the block
    char memblock[File::block_size + 1];
    file.seekg(start, ios_base::beg);
    file.read(memblock, len);
    file.close();
    memblock[len] = '\0';
    _return.assign(memblock);
    return;
  }
}

// Checks the number of blocks in the local partition
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

void LocalFileRegistry::recordComplete(const JobID j, const PartID p, const string n){
  FileMap::accessor acc_part;
  this->file_map.insert(acc_part, p);
  acc_part->second.push_back(File(j,p,n));
}


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


Transfer::Transfer(PartID p, Location l): pid(p), location(l), t_status(transferstatus::DNE){}

Status Transfer::getFile(string outputfile){
  cout << "Starting to transfer file..."  << endl;
  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);

  // Set up the transfer this is the first call
  cout << "Checking status..."  << endl;
  if(t_status == transferstatus::DNE){
    total = client.blockCount(pid);
    if(total < 0){ // Job not done, comeback later.
      cout << "Not ready."   << endl;
      return transferstatus::DNE;
    }
    progress = 0; // Let's get ready to rock.
    transferstatus::READY;
  }

  cout << "Done."   << endl;
  if(progress == total){ // Hey. We're done.
    t_status = transferstatus::DONE;
    return transferstatus::DONE;
  }

  // Get the next block and append.
  cout << "Fetching..."  << endl;
  string buffer;
  client.sendData(buffer, pid, progress);
  cout << "...Caught."  << endl;
  fstream fs (outputfile.c_str(), fstream::app | fstream::out);
  assert(!fs.fail());
  fs << buffer;
  fs.close();
  progress++;
}

Status Transfer::checkStatus(){
  cout << "Setting up with..." << location.ip << ":" << location.port  << endl;
  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);

  // Set up the transfer this is the first call
  cout << "Checking status..."  << endl;
  if(t_status == transferstatus::DNE){
    total = client.blockCount(pid);
    if(total < 0){ // Job not done, comeback later.
      cout << "Not ready."   << endl;
      return transferstatus::DNE;
    }
    progress = 0; // Let's get ready to rock.
    transferstatus::READY;
  }

  cout << "Done."   << endl;
  if(progress == total){ // Hey. We're done.
    t_status = transferstatus::DONE;
    return transferstatus::DONE;
  }

  return transferstatus::READY;
}
