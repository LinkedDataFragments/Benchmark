#!/bin/bash
# Keeps retrying ssh until success

# Check we've got command line arguments
if [ -z "$*" ] ; then
    echo "Need to specify ssh options"
    exit 1
fi

# Start trying and retrying
rc=255
while [[ $rc -eq 255 ]] ; do
    sshpass $*
    rc=$?
    if [[ $rc -ne 0 ]]; then
        >&2 echo "Connection failed with exit code $rc, waiting 1 second (retrying if 255)..."
        sleep 1
    fi
done