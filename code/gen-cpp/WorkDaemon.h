/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef WorkDaemon_H
#define WorkDaemon_H

#include <TProcessor.h>
#include "daemon_types.h"

namespace workdaemon {

class WorkDaemonIf {
 public:
  virtual ~WorkDaemonIf() {}
  virtual void bark(const std::string& s) = 0;
  virtual void stateString(std::string& _return) = 0;
  virtual void kill() = 0;
  virtual void listStatus(std::map<JobID, Status> & _return) = 0;
  virtual void startMapper(const JobID jid, const std::string& inFile, const ChunkID cid) = 0;
  virtual void startReducer(const JobID jid, const PartID pid, const std::string& outFile) = 0;
  virtual void sendData(std::string& _return, const PartID kid, const BlockID bid) = 0;
  virtual Status partitionStatus(const PartID pid) = 0;
  virtual Count blockCount(const PartID pid) = 0;
  virtual void reportCompletedJobs(const std::vector<URL> & done) = 0;
};

class WorkDaemonNull : virtual public WorkDaemonIf {
 public:
  virtual ~WorkDaemonNull() {}
  void bark(const std::string& /* s */) {
    return;
  }
  void stateString(std::string& /* _return */) {
    return;
  }
  void kill() {
    return;
  }
  void listStatus(std::map<JobID, Status> & /* _return */) {
    return;
  }
  void startMapper(const JobID /* jid */, const std::string& /* inFile */, const ChunkID /* cid */) {
    return;
  }
  void startReducer(const JobID /* jid */, const PartID /* pid */, const std::string& /* outFile */) {
    return;
  }
  void sendData(std::string& /* _return */, const PartID /* kid */, const BlockID /* bid */) {
    return;
  }
  Status partitionStatus(const PartID /* pid */) {
    Status _return = 0;
    return _return;
  }
  Count blockCount(const PartID /* pid */) {
    Count _return = 0;
    return _return;
  }
  void reportCompletedJobs(const std::vector<URL> & /* done */) {
    return;
  }
};

class WorkDaemon_bark_args {
 public:

  WorkDaemon_bark_args() : s("") {
  }

  virtual ~WorkDaemon_bark_args() throw() {}

  std::string s;

  struct __isset {
    __isset() : s(false) {}
    bool s;
  } __isset;

  bool operator == (const WorkDaemon_bark_args & rhs) const
  {
    if (!(s == rhs.s))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_bark_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_bark_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_bark_pargs {
 public:


  virtual ~WorkDaemon_bark_pargs() throw() {}

  const std::string* s;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_stateString_args {
 public:

  WorkDaemon_stateString_args() {
  }

  virtual ~WorkDaemon_stateString_args() throw() {}


  bool operator == (const WorkDaemon_stateString_args & /* rhs */) const
  {
    return true;
  }
  bool operator != (const WorkDaemon_stateString_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_stateString_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_stateString_pargs {
 public:


  virtual ~WorkDaemon_stateString_pargs() throw() {}


  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_stateString_result {
 public:

  WorkDaemon_stateString_result() : success("") {
  }

  virtual ~WorkDaemon_stateString_result() throw() {}

  std::string success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const WorkDaemon_stateString_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_stateString_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_stateString_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_stateString_presult {
 public:


  virtual ~WorkDaemon_stateString_presult() throw() {}

  std::string* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_kill_args {
 public:

  WorkDaemon_kill_args() {
  }

  virtual ~WorkDaemon_kill_args() throw() {}


  bool operator == (const WorkDaemon_kill_args & /* rhs */) const
  {
    return true;
  }
  bool operator != (const WorkDaemon_kill_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_kill_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_kill_pargs {
 public:


  virtual ~WorkDaemon_kill_pargs() throw() {}


  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_listStatus_args {
 public:

  WorkDaemon_listStatus_args() {
  }

  virtual ~WorkDaemon_listStatus_args() throw() {}


  bool operator == (const WorkDaemon_listStatus_args & /* rhs */) const
  {
    return true;
  }
  bool operator != (const WorkDaemon_listStatus_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_listStatus_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_listStatus_pargs {
 public:


  virtual ~WorkDaemon_listStatus_pargs() throw() {}


  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_listStatus_result {
 public:

  WorkDaemon_listStatus_result() {
  }

  virtual ~WorkDaemon_listStatus_result() throw() {}

  std::map<JobID, Status>  success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const WorkDaemon_listStatus_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_listStatus_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_listStatus_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_listStatus_presult {
 public:


  virtual ~WorkDaemon_listStatus_presult() throw() {}

  std::map<JobID, Status> * success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_startMapper_args {
 public:

  WorkDaemon_startMapper_args() : jid(0), inFile(""), cid(0) {
  }

  virtual ~WorkDaemon_startMapper_args() throw() {}

  JobID jid;
  std::string inFile;
  ChunkID cid;

  struct __isset {
    __isset() : jid(false), inFile(false), cid(false) {}
    bool jid;
    bool inFile;
    bool cid;
  } __isset;

  bool operator == (const WorkDaemon_startMapper_args & rhs) const
  {
    if (!(jid == rhs.jid))
      return false;
    if (!(inFile == rhs.inFile))
      return false;
    if (!(cid == rhs.cid))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_startMapper_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_startMapper_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_startMapper_pargs {
 public:


  virtual ~WorkDaemon_startMapper_pargs() throw() {}

  const JobID* jid;
  const std::string* inFile;
  const ChunkID* cid;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_startReducer_args {
 public:

  WorkDaemon_startReducer_args() : jid(0), pid(0), outFile("") {
  }

  virtual ~WorkDaemon_startReducer_args() throw() {}

  JobID jid;
  PartID pid;
  std::string outFile;

  struct __isset {
    __isset() : jid(false), pid(false), outFile(false) {}
    bool jid;
    bool pid;
    bool outFile;
  } __isset;

  bool operator == (const WorkDaemon_startReducer_args & rhs) const
  {
    if (!(jid == rhs.jid))
      return false;
    if (!(pid == rhs.pid))
      return false;
    if (!(outFile == rhs.outFile))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_startReducer_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_startReducer_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_startReducer_pargs {
 public:


  virtual ~WorkDaemon_startReducer_pargs() throw() {}

  const JobID* jid;
  const PartID* pid;
  const std::string* outFile;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_sendData_args {
 public:

  WorkDaemon_sendData_args() : kid(0), bid(0) {
  }

  virtual ~WorkDaemon_sendData_args() throw() {}

  PartID kid;
  BlockID bid;

  struct __isset {
    __isset() : kid(false), bid(false) {}
    bool kid;
    bool bid;
  } __isset;

  bool operator == (const WorkDaemon_sendData_args & rhs) const
  {
    if (!(kid == rhs.kid))
      return false;
    if (!(bid == rhs.bid))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_sendData_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_sendData_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_sendData_pargs {
 public:


  virtual ~WorkDaemon_sendData_pargs() throw() {}

  const PartID* kid;
  const BlockID* bid;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_sendData_result {
 public:

  WorkDaemon_sendData_result() : success("") {
  }

  virtual ~WorkDaemon_sendData_result() throw() {}

  std::string success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const WorkDaemon_sendData_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_sendData_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_sendData_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_sendData_presult {
 public:


  virtual ~WorkDaemon_sendData_presult() throw() {}

  std::string* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_partitionStatus_args {
 public:

  WorkDaemon_partitionStatus_args() : pid(0) {
  }

  virtual ~WorkDaemon_partitionStatus_args() throw() {}

  PartID pid;

  struct __isset {
    __isset() : pid(false) {}
    bool pid;
  } __isset;

  bool operator == (const WorkDaemon_partitionStatus_args & rhs) const
  {
    if (!(pid == rhs.pid))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_partitionStatus_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_partitionStatus_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_partitionStatus_pargs {
 public:


  virtual ~WorkDaemon_partitionStatus_pargs() throw() {}

  const PartID* pid;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_partitionStatus_result {
 public:

  WorkDaemon_partitionStatus_result() : success(0) {
  }

  virtual ~WorkDaemon_partitionStatus_result() throw() {}

  Status success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const WorkDaemon_partitionStatus_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_partitionStatus_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_partitionStatus_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_partitionStatus_presult {
 public:


  virtual ~WorkDaemon_partitionStatus_presult() throw() {}

  Status* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_blockCount_args {
 public:

  WorkDaemon_blockCount_args() : pid(0) {
  }

  virtual ~WorkDaemon_blockCount_args() throw() {}

  PartID pid;

  struct __isset {
    __isset() : pid(false) {}
    bool pid;
  } __isset;

  bool operator == (const WorkDaemon_blockCount_args & rhs) const
  {
    if (!(pid == rhs.pid))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_blockCount_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_blockCount_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_blockCount_pargs {
 public:


  virtual ~WorkDaemon_blockCount_pargs() throw() {}

  const PartID* pid;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_blockCount_result {
 public:

  WorkDaemon_blockCount_result() : success(0) {
  }

  virtual ~WorkDaemon_blockCount_result() throw() {}

  Count success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const WorkDaemon_blockCount_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_blockCount_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_blockCount_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_blockCount_presult {
 public:


  virtual ~WorkDaemon_blockCount_presult() throw() {}

  Count* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_reportCompletedJobs_args {
 public:

  WorkDaemon_reportCompletedJobs_args() {
  }

  virtual ~WorkDaemon_reportCompletedJobs_args() throw() {}

  std::vector<URL>  done;

  struct __isset {
    __isset() : done(false) {}
    bool done;
  } __isset;

  bool operator == (const WorkDaemon_reportCompletedJobs_args & rhs) const
  {
    if (!(done == rhs.done))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_reportCompletedJobs_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_reportCompletedJobs_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_reportCompletedJobs_pargs {
 public:


  virtual ~WorkDaemon_reportCompletedJobs_pargs() throw() {}

  const std::vector<URL> * done;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemonClient : virtual public WorkDaemonIf {
 public:
  WorkDaemonClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) :
    piprot_(prot),
    poprot_(prot) {
    iprot_ = prot.get();
    oprot_ = prot.get();
  }
  WorkDaemonClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) :
    piprot_(iprot),
    poprot_(oprot) {
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  void bark(const std::string& s);
  void send_bark(const std::string& s);
  void stateString(std::string& _return);
  void send_stateString();
  void recv_stateString(std::string& _return);
  void kill();
  void send_kill();
  void listStatus(std::map<JobID, Status> & _return);
  void send_listStatus();
  void recv_listStatus(std::map<JobID, Status> & _return);
  void startMapper(const JobID jid, const std::string& inFile, const ChunkID cid);
  void send_startMapper(const JobID jid, const std::string& inFile, const ChunkID cid);
  void startReducer(const JobID jid, const PartID pid, const std::string& outFile);
  void send_startReducer(const JobID jid, const PartID pid, const std::string& outFile);
  void sendData(std::string& _return, const PartID kid, const BlockID bid);
  void send_sendData(const PartID kid, const BlockID bid);
  void recv_sendData(std::string& _return);
  Status partitionStatus(const PartID pid);
  void send_partitionStatus(const PartID pid);
  Status recv_partitionStatus();
  Count blockCount(const PartID pid);
  void send_blockCount(const PartID pid);
  Count recv_blockCount();
  void reportCompletedJobs(const std::vector<URL> & done);
  void send_reportCompletedJobs(const std::vector<URL> & done);
 protected:
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
};

class WorkDaemonProcessor : virtual public ::apache::thrift::TProcessor {
 protected:
  boost::shared_ptr<WorkDaemonIf> iface_;
  virtual bool process_fn(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, std::string& fname, int32_t seqid);
 private:
  std::map<std::string, void (WorkDaemonProcessor::*)(int32_t, ::apache::thrift::protocol::TProtocol*, ::apache::thrift::protocol::TProtocol*)> processMap_;
  void process_bark(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_stateString(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_kill(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_listStatus(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_startMapper(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_startReducer(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_sendData(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_partitionStatus(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_blockCount(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_reportCompletedJobs(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
 public:
  WorkDaemonProcessor(boost::shared_ptr<WorkDaemonIf> iface) :
    iface_(iface) {
    processMap_["bark"] = &WorkDaemonProcessor::process_bark;
    processMap_["stateString"] = &WorkDaemonProcessor::process_stateString;
    processMap_["kill"] = &WorkDaemonProcessor::process_kill;
    processMap_["listStatus"] = &WorkDaemonProcessor::process_listStatus;
    processMap_["startMapper"] = &WorkDaemonProcessor::process_startMapper;
    processMap_["startReducer"] = &WorkDaemonProcessor::process_startReducer;
    processMap_["sendData"] = &WorkDaemonProcessor::process_sendData;
    processMap_["partitionStatus"] = &WorkDaemonProcessor::process_partitionStatus;
    processMap_["blockCount"] = &WorkDaemonProcessor::process_blockCount;
    processMap_["reportCompletedJobs"] = &WorkDaemonProcessor::process_reportCompletedJobs;
  }

  virtual bool process(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot);
  virtual ~WorkDaemonProcessor() {}
};

class WorkDaemonMultiface : virtual public WorkDaemonIf {
 public:
  WorkDaemonMultiface(std::vector<boost::shared_ptr<WorkDaemonIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~WorkDaemonMultiface() {}
 protected:
  std::vector<boost::shared_ptr<WorkDaemonIf> > ifaces_;
  WorkDaemonMultiface() {}
  void add(boost::shared_ptr<WorkDaemonIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  void bark(const std::string& s) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->bark(s);
    }
  }

  void stateString(std::string& _return) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->stateString(_return);
        return;
      } else {
        ifaces_[i]->stateString(_return);
      }
    }
  }

  void kill() {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->kill();
    }
  }

  void listStatus(std::map<JobID, Status> & _return) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->listStatus(_return);
        return;
      } else {
        ifaces_[i]->listStatus(_return);
      }
    }
  }

  void startMapper(const JobID jid, const std::string& inFile, const ChunkID cid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->startMapper(jid, inFile, cid);
    }
  }

  void startReducer(const JobID jid, const PartID pid, const std::string& outFile) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->startReducer(jid, pid, outFile);
    }
  }

  void sendData(std::string& _return, const PartID kid, const BlockID bid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->sendData(_return, kid, bid);
        return;
      } else {
        ifaces_[i]->sendData(_return, kid, bid);
      }
    }
  }

  Status partitionStatus(const PartID pid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->partitionStatus(pid);
      } else {
        ifaces_[i]->partitionStatus(pid);
      }
    }
  }

  Count blockCount(const PartID pid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->blockCount(pid);
      } else {
        ifaces_[i]->blockCount(pid);
      }
    }
  }

  void reportCompletedJobs(const std::vector<URL> & done) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->reportCompletedJobs(done);
    }
  }

};

} // namespace

#endif
