/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef Master_H
#define Master_H

#include <TProcessor.h>
#include "master_types.h"

namespace Master {

class MasterIf {
 public:
  virtual ~MasterIf() {}
  virtual void ping() = 0;
  virtual void bark(const std::string& s) = 0;
};

class MasterNull : virtual public MasterIf {
 public:
  virtual ~MasterNull() {}
  void ping() {
    return;
  }
  void bark(const std::string& /* s */) {
    return;
  }
};

class Master_ping_args {
 public:

  Master_ping_args() {
  }

  virtual ~Master_ping_args() throw() {}


  bool operator == (const Master_ping_args & /* rhs */) const
  {
    return true;
  }
  bool operator != (const Master_ping_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Master_ping_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class Master_ping_pargs {
 public:


  virtual ~Master_ping_pargs() throw() {}


  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class Master_bark_args {
 public:

  Master_bark_args() : s("") {
  }

  virtual ~Master_bark_args() throw() {}

  std::string s;

  struct __isset {
    __isset() : s(false) {}
    bool s;
  } __isset;

  bool operator == (const Master_bark_args & rhs) const
  {
    if (!(s == rhs.s))
      return false;
    return true;
  }
  bool operator != (const Master_bark_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Master_bark_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class Master_bark_pargs {
 public:


  virtual ~Master_bark_pargs() throw() {}

  const std::string* s;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

class MasterClient : virtual public MasterIf {
 public:
  MasterClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) :
    piprot_(prot),
    poprot_(prot) {
    iprot_ = prot.get();
    oprot_ = prot.get();
  }
  MasterClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) :
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
  void ping();
  void send_ping();
  void bark(const std::string& s);
  void send_bark(const std::string& s);
 protected:
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
};

class MasterProcessor : virtual public ::apache::thrift::TProcessor {
 protected:
  boost::shared_ptr<MasterIf> iface_;
  virtual bool process_fn(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, std::string& fname, int32_t seqid);
 private:
  std::map<std::string, void (MasterProcessor::*)(int32_t, ::apache::thrift::protocol::TProtocol*, ::apache::thrift::protocol::TProtocol*)> processMap_;
  void process_ping(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
  void process_bark(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot);
 public:
  MasterProcessor(boost::shared_ptr<MasterIf> iface) :
    iface_(iface) {
    processMap_["ping"] = &MasterProcessor::process_ping;
    processMap_["bark"] = &MasterProcessor::process_bark;
  }

  virtual bool process(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot);
  virtual ~MasterProcessor() {}
};

class MasterMultiface : virtual public MasterIf {
 public:
  MasterMultiface(std::vector<boost::shared_ptr<MasterIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~MasterMultiface() {}
 protected:
  std::vector<boost::shared_ptr<MasterIf> > ifaces_;
  MasterMultiface() {}
  void add(boost::shared_ptr<MasterIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  void ping() {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->ping();
    }
  }

  void bark(const std::string& s) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->bark(s);
    }
  }

};

} // namespace

#endif
