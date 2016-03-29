#!/bin/bash
# Only shows lines *after* $2 occurences of $1 have been found.

count=0
while read line; do
    if [ "$count" == "$2" ]; then
        echo $line
    fi
    if [[ "${line}" == *$1* ]]; then
        let count++
    fi
done