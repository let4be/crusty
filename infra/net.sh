#!/bin/bash

cd $(git rev-parse --show-toplevel)

conf () {
	INDEX=$1
	IP=$2
	echo "Configuring $DEV:$INDEX with $IP"
	ifconfig $DEV:$INDEX $IP up
}

echo "what is the NIC name?"
read -r DEV
echo "about to configure $DEV"

echo "what are NIC private IPs?"
i=0
while IFS='$\n' read -r IP; do
    if [ "$IP" == "" ]; then
        break
    fi
    echo "about to configure $DEV with $IP"

    while true; do
        read -p "Continue(y/n)?" yn
        case $yn in
            [Yy]* ) conf $i "$IP"; break;;
            [Nn]* ) break;;
            * ) echo "Please answer yes or no.";;
        esac
    done

    ((i=i+1))
    echo "next ip?"
done

cp  ./infra/sysctl.conf /etc/sysctl.d/90-crusty.conf
sysctl -p
