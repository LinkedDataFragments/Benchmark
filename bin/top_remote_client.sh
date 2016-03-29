#!/bin/bash
# Collect top data from the given pid in the running docker container with name "client"

# Check for correct usage
if [ $# -ne 4 ]; then
    echo "Wrong usage" >&2
    echo $0" [location] [username] [password] [pid]" >&2
    exit 1
fi

location=$1
username=$2
password=$3
pid=$4

# -oStrictHostKeyChecking=no   This always accepts server fingerprint
sshpass -p "$password" ssh -t -t -oStrictHostKeyChecking=no $username@$location <<ENDSSH | ./filteruntil.sh "---START---" 2
sudo docker exec -i client /bin/bash <<ENDDOCKER
echo "---START---"
top -p $pid -b -d 1 | gawk '{if(\\\$1 == '$pid'){ print \\\$1 " " \\\$9 " " \\\$6 "K ";system(""); }}'
ENDDOCKER
exit
ENDSSH