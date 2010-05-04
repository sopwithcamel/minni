#!/bin/bash

if [ -e hello.txt ]
then
	rm hello.txt
fi

text="`cat hello.sample`"

for ((i = 0; i < $1; i++))
do
	text="${text} ${text}"
done

echo -e "$text" >> hello.txt
