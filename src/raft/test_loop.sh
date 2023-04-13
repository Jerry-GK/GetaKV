#!/bin/bash

for i in {1..500}; do
    echo "Running test $i"
    echo "Running test $i" >>out
    go test -run 2B >>out
    echo "Finished test $i"
    echo "==========================="
    echo "Finished test $i" >>out
    echo "===========================" >>out
done
