#!/bin/bash
# Collect top data from the given pid in the running docker container with name "client"

# Check for correct usage
if [ $# -ne 5 ]; then
    echo "Wrong usage" >&2
    echo $0" [location] [username] [password] [localid] [pid]" >&2
    exit 1
fi

location=$1
username=$2
password=$3
localid=$4
pid=$5

# -oStrictHostKeyChecking=no   This always accepts server fingerprint
./retrying-sshpass.sh -p "$password" ssh -t -t -oStrictHostKeyChecking=no $username@$location <<ENDSSH | ./filteruntil.sh "---START---" 3
sudo docker exec -i client-$localid /bin/bash <<ENDDOCKER
echo "---START---"
top -p $pid -b -d 1 | gawk '{if(\\\$1 == '$pid'){ print \\\$1 " " \\\$9 " " \\\$6 "K ";system(""); }}'
ENDDOCKER
exit
ENDSSH