#!/bin/bash

tested="2B"
filename="out"
iterNum=500

echo "New Batch Test" >$filename

i=1
while [ $i -le $iterNum ]; do
    echo "Running test $i"
    echo "Running test $i" >>$filename
    go test -run $tested >>out
    echo "Finished test $i"
    echo "==========================="
    echo "Finished test $i" >>$filename
    echo "===========================" >>$filename
    i=$((i + 1))
done
