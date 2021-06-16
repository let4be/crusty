#!/bin/bash

set -e

conf () {
	DEV=$1
	GW=$2
	IP=$(/sbin/ip -o -4 addr list $DEV | awk '{print $4}' | cut -d/ -f1)
	N=$3
	echo "Configuring $DEV with GW $GW and IP $IP table $N"
	ip route add default via $GW dev $DEV tab $N
	ip rule add from $IP tab $N
}

conf "ens5" "172.31.0.1" 1
conf "ens6" "172.32.0.1" 2

echo "net.ipv4.ip_local_port_range = 1024 65535" >> /etc/sysctl.conf
echo "net.ipv4.tcp_tw_reuse = 1" >> /etc/sysctl.conf
sysctl -p
