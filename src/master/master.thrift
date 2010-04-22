#!/usr/local/bin/thrift --gen cpp

namespace cpp Test

service Something {
  oneway void ping()
}
