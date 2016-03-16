#!/bin/bash

# Check for correct usage
if [ $# -ne 1 ]; then
    echo "Wrong usage" >&2
    echo $0" [pid]" >&2
    exit 1
fi

PID=$1

# Check if PID is number
re='^[0-9]+$'
if ! [[ $1 =~ $PID ]] ; then
    echo "Error: [pid] must be a number" >&2
    exit 1
fi

# Check if valid PID
if ! ps -p $PID > /dev/null ; then
   echo "Error: [pid] must be the process id of a running process" >&2
   exit 1
fi

# Start logging top output
if [ "$(uname)" == "Darwin" ]; then
    top -pid $PID -stats pid,cpu,mem -d -l 0 -s 5 | gawk '{if($1 == '$PID'){ print;system(""); }}' &
    sub=$!
else
    top -p $PID -b -d 1 | gawk '{if($1 == '$PID'){ print $1 " " $9 " " $6 "K ";system(""); }}' &
    sub=$!
fi

# Force-kill top to avoid orphan processes
trap "kill -9 $sub" SIGINT SIGTERM EXIT
wait $sub