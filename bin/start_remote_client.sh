#!/bin/bash
# Start a docker container with name "client"

# Check for correct usage
if [ $# -ne 9 ]; then
    echo "Wrong usage" >&2
    echo $0" [location] [username] [password] [debug] [type] [interval] [caching] [query] [target]" >&2
    exit 1
fi

location=$1
username=$2
password=$3
debug=$4
type=$5
interval=$6
caching=$7
query=$8
target=$9

# -oStrictHostKeyChecking=no   This always accepts server fingerprint
sshpass -p "$password" ssh -t -t -oStrictHostKeyChecking=no $username@$location <<ENDSSH
sudo docker run --name "client" \
-e "QUERY=$query" \
-e "TARGET=$target" \
-e "CACHING=$caching" \
-e "INTERVAL=$interval" \
-e "DEBUG=$debug" \
--rm tpfqs-client $type
exit
ENDSSH

