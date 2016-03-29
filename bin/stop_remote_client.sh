#!/bin/bash
# Stop a docker container with name "client"

# Check for correct usage
if [ $# -ne 3 ]; then
    echo "Wrong usage" >&2
    echo $0" [location] [username] [password]" >&2
    exit 1
fi

location=$1
username=$2
password=$3

# -oStrictHostKeyChecking=no   This always accepts server fingerprint
sshpass -p "$password" ssh -t -t -oStrictHostKeyChecking=no $username@$location <<ENDSSH
sudo docker stop client
exit
ENDSSH

