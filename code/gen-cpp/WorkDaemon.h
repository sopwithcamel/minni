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
  virtual void pulse(std::map<JobID, Status> & _return) = 0;
  virtual void startMapper(const JobID jid, const ChunkID cid) = 0;
  virtual void startReducer(const JobID jid, const KeyID kid, const std::string& outFile) = 0;
  virtual void sendData(std::vector<std::string> & _return, const JobID jid, const KeyID kid, const SeriesID sid) = 0;
  virtual void kill(const JobID jid) = 0;
};

class WorkDaemonNull : virtual public WorkDaemonIf {
 public:
  virtual ~WorkDaemonNull() {}
  void bark(const std::string& /* s */) {
    return;
  }
  void pulse(std::map<JobID, Status> & /* _return */) {
    return;
  }
  void startMapper(const JobID /* jid */, const ChunkID /* cid */) {
    return;
  }
  void startReducer(const JobID /* jid */, const KeyID /* kid */, const std::string& /* outFile */) {
    return;
  }
  void sendData(std::vector<std::string> & /* _return */, const JobID /* jid */, const KeyID /* kid */, const SeriesID /* sid */) {
    return;
  }
  void kill(const JobID /* jid */) {
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

class WorkDaemon_pulse_args {
 public:

  WorkDaemon_pulse_args() {
  }

  virtual ~WorkDaemon_pulse_args() throw() {}


  bool operator == (const WorkDaemon_pulse_args & /* rhs */) const
  {
    return true;
  }
  bool operator != (const WorkDaemon_pulse_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_pulse_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_pulse_pargs {
 public:


  virtual ~WorkDaemon_pulse_pargs() throw() {}


  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_pulse_result {
 public:

  WorkDaemon_pulse_result() {
  }

  virtual ~WorkDaemon_pulse_result() throw() {}

  std::map<JobID, Status>  success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const WorkDaemon_pulse_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const WorkDaemon_pulse_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const WorkDaemon_pulse_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_pulse_presult {
 public:


  virtual ~WorkDaemon_pulse_presult() throw() {}

  std::map<JobID, Status> * success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_startMapper_args {
 public:

  WorkDaemon_startMapper_args() : jid(0), cid(0) {
  }

  virtual ~WorkDaemon_startMapper_args() throw() {}

  JobID jid;
  ChunkID cid;

  struct __isset {
    __isset() : jid(false), cid(false) {}
    bool jid;
    bool cid;
  } __isset;

  bool operator == (const WorkDaemon_startMapper_args & rhs) const
  {
    if (!(jid == rhs.jid))
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
  const ChunkID* cid;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_startReducer_args {
 public:

  WorkDaemon_startReducer_args() : jid(0), kid(0), outFile("") {
  }

  virtual ~WorkDaemon_startReducer_args() throw() {}

  JobID jid;
  KeyID kid;
  std::string outFile;

  struct __isset {
    __isset() : jid(false), kid(false), outFile(false) {}
    bool jid;
    bool kid;
    bool outFile;
  } __isset;

  bool operator == (const WorkDaemon_startReducer_args & rhs) const
  {
    if (!(jid == rhs.jid))
      return false;
    if (!(kid == rhs.kid))
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
  const KeyID* kid;
  const std::string* outFile;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_sendData_args {
 public:

  WorkDaemon_sendData_args() : jid(0), kid(0), sid(0) {
  }

  virtual ~WorkDaemon_sendData_args() throw() {}

  JobID jid;
  KeyID kid;
  SeriesID sid;

  struct __isset {
    __isset() : jid(false), kid(false), sid(false) {}
    bool jid;
    bool kid;
    bool sid;
  } __isset;

  bool operator == (const WorkDaemon_sendData_args & rhs) const
  {
    if (!(jid == rhs.jid))
      return false;
    if (!(kid == rhs.kid))
      return false;
    if (!(sid == rhs.sid))
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

  const JobID* jid;
  const KeyID* kid;
  const SeriesID* sid;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class WorkDaemon_sendData_result {
 public:

  WorkDaemon_sendData_result() {
  }

  virtual ~WorkDaemon_sendData_result() throw() {}

  std::vector<std::string>  success;

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

  std::vector<std::string> * success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class WorkDaemon_kill_args {
 public:

  WorkDaemon_kill_args() : jid(0) {
  }

  virtual ~WorkDaemon_kill_args() throw() {}

  JobID jid;

  struct __isset {
    __isset() : jid(false) {}
    bool jid;
  } __isset;

  bool operator == (const WorkDaemon_kill_args & rhs) const
  {
    if (!(jid == rhs.jid))
      return false;
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

  const JobID* jid;

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
  void pulse(std::map<JobID, Status> & _return);
  void send_pulse();
  void recv_pulse(std::map<JobID, Status> & _return);
  void startMapper(const JobID jid, const ChunkID cid);
  void send_startMapper(const JobID jid, const ChunkID cid);
  void startReducer(const JobID jid, const KeyID kid, const std::string& outFile);
  void send_startReducer(const JobID jid, const KeyID kid, const std::string& outFile);
  void sendData(std::vector<std::string> & _return, const JobID jid, const KeyID kid, const SeriesID sid);
  void send_sendData(const JobID jid, const KeyID kid, const SeriesID sid);
  void recv_sendData(std::vector<std::string> & _return);
  void kill(const JobID jid);
  void send_kill(const JobID jid);
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
  void process_pulse(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_startMapper(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_startReducer(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_sendData(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_kill(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
 public:
  WorkDaemonProcessor(boost::shared_ptr<WorkDaemonIf> iface) :
    iface_(iface) {
    processMap_["bark"] = &WorkDaemonProcessor::process_bark;
    processMap_["pulse"] = &WorkDaemonProcessor::process_pulse;
    processMap_["startMapper"] = &WorkDaemonProcessor::process_startMapper;
    processMap_["startReducer"] = &WorkDaemonProcessor::process_startReducer;
    processMap_["sendData"] = &WorkDaemonProcessor::process_sendData;
    processMap_["kill"] = &WorkDaemonProcessor::process_kill;
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

  void pulse(std::map<JobID, Status> & _return) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->pulse(_return);
        return;
      } else {
        ifaces_[i]->pulse(_return);
      }
    }
  }

  void startMapper(const JobID jid, const ChunkID cid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->startMapper(jid, cid);
    }
  }

  void startReducer(const JobID jid, const KeyID kid, const std::string& outFile) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->startReducer(jid, kid, outFile);
    }
  }

  void sendData(std::vector<std::string> & _return, const JobID jid, const KeyID kid, const SeriesID sid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->sendData(_return, jid, kid, sid);
        return;
      } else {
        ifaces_[i]->sendData(_return, jid, kid, sid);
      }
    }
  }

  void kill(const JobID jid) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->kill(jid);
    }
  }

};

} // namespace

#endif
