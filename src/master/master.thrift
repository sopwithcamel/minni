#!/usr/local/bin/thrift --gen cpp

namespace cpp Master

service Master {
	oneway void bark(1:string s) // Debug
}
