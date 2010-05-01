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


Transfer::Transfer(PartID p, Location l, string o): 
  pid(p), location(l), outfile(o),
  t_status(transferstatus::DNE), progress(0), total(-1){
  ofstream clearing(o.c_str(), ios::trunc|ios::out);
  clearing.close();
}

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

Status Transfer::checkStatus(){
  boost::shared_ptr<TSocket> socket(new TSocket(location.ip, location.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	
  workdaemon::WorkDaemonClient client(protocol);

  // We can't become undone
  if(t_status == transferstatus::DONE){
    return t_status;
  }

  // Check for new blocks
  transport->open();
  total = client.blockCount(pid); // update count
  transport->close();

  // Don't have any new ones
  if(progress == total){
    transport->open();
    Status part_status = client.dataStatus(pid);
    transport->close();
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
