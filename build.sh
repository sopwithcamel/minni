#!/bin/bash

LDFLAGS=-L/home/wolf/Desktop/kosmostest/kfs-0.5/build/lib CPPFLAGS=-I/home/wolf/Desktop/kosmostest/kfs-0.5/build/include/kfs/ ./configure --enable-silent-rules

make
