#!/bin/bash
# This is a script for batch testing

testCommand="go test -run TestChallenge1Concurrent -race"
outputFile="log_all_2.txt"
iterNum=500
timeoutDuration="300s"  # 5 minutes

rm -f $outputFile
touch $outputFile
echo "New Batch Test" | tee -a $outputFile

i=1
while [ $i -le $iterNum ]; do
    echo "Running test $i" | tee -a $outputFile
    if timeout $timeoutDuration $testCommand | tee -a $outputFile; then
        echo "Finished test $i, Timeout!" | tee -a $outputFile
    else
       echo "Not finished test $i ?" | tee -a $outputFile 
    fi
    echo "===========================" | tee -a $outputFile
    i=$((i + 1))
done