#!/bin/bash


if [ ! -d m4 ]
then
	mkdir m4
fi

if [ ! -d build-aux ]
then
	mkdir build-aux
fi

if [ ! -d src/common ]
then
	mkdir src/common
fi

make -k maintainer-clean
autoreconf --install --force
