#!/bin/bash
# This is a script for batch testing

testCommand="go test -run TestConcurrent2 -race"
outputFile="out"
iterNum=300

echo "New Batch Test" >$outputFile

i=1
while [ $i -le $iterNum ]; do
    echo "Running test $i" | tee $outputFile
    $testCommand | tee $outputFile
    echo "Finished test $i" | tee $outputFile
    echo "===========================" | tee $outputFile
    i=$((i + 1))
done
