#!/bin/bash
# This is a script for batch testing

testCommand="go test -race"
outputFile="out"
iterNum=300

echo "New Batch Test" >$outputFile

i=1
while [ $i -le $iterNum ]; do
    echo "Running test $i"
    echo "Running test $i" >>$outputFile
    $testCommand >>$outputFile
    echo "Finished test $i"
    echo "==========================="
    echo "Finished test $i" >>$outputFile
    echo "===========================" >>$outputFile
    i=$((i + 1))
done
