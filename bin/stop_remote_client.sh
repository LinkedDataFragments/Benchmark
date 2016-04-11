#!/bin/bash
# Stop a docker container with name "client"

# Check for correct usage
if [ $# -ne 4 ]; then
    echo "Wrong usage" >&2
    echo $0" [location] [username] [password] [local-id]" >&2
    exit 1
fi

location=$1
username=$2
password=$3
localid=$4

# -oStrictHostKeyChecking=no   This always accepts server fingerprint
./retrying-sshpass.sh -p "$password" ssh -t -t -oStrictHostKeyChecking=no $username@$location <<ENDSSH
sudo docker stop client-$localid
sudo docker rm client-$localid
exit
ENDSSH

